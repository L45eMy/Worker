#!/usr/bin/python

import logging
import json
import requests
import dateutil.parser

from backend import Backend
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.WARNING)

logger = logging.getLogger('scheduler')
logger.setLevel(level=logging.INFO)

def dict_merge(first, second):
	import copy
	for k, v in second.iteritems():
		if k in first and isinstance(first[k], dict):
			dict_merge(first[k], v)
		else:
			first[k] = copy.deepcopy(v)
	return first


class Scheduler(object):

	# structure = {
	# 	'type': IS(*TYPE.values_both_types()),
	# 	'state': IS(*STATE.values_both_types()),
	# 	'jobInfo': dict,
	# 		accountId, bundleId, storeCountry, appType{appstore,cydia}
	# 	'worker': Worker,
	# 	'device': Device,
	# 	'date_added': float
	# }

	@classmethod
	def _default_runjob(cls):
		return {
			'type':'run_app',
			'state':'pending',
			'jobInfo': {
				'appType':'AppStoreApp'
			},
		}

	@classmethod
	def _default_dioscopejob(cls):
		return {
			'type':'dioscope',
			'state':'pending',
			'jobInfo': {
				'appType':'AppStoreApp'
			}
		}

	def __init__(self, backendUrl):
		self.backend = Backend(backendUrl)

	def schedule_job(self, jobDict, type='run'):
		if type == 'dioscope':
			job = Scheduler._default_dioscopejob()
		else:
			job = Scheduler._default_runjob()
		job = dict_merge(job, jobDict)
		jobId = self.backend.post_job(job)
		return jobId


	def schedule_bundleId(self, bundleId, worker=None, device=None, account=None, country=None, executionStrategy=None, type='run'):
		jobDict = {
			'jobInfo': {
				'bundleId':bundleId
			}
		}
		if worker:
			jobDict['worker'] = worker
		if device:
			jobDict['device'] = device
		if account:
			jobDict['jobInfo']['accountId'] = account
		if country:
			jobDict['jobInfo']['storeCountry'] = country
		if executionStrategy:
			jobDict['jobInfo']['executionStrategy'] = executionStrategy
		return self.schedule_job(jobDict, type)


	def schedule_appId(self, appId, account=None, country=None, executionStrategy=None):
		url = 'http://itunes.apple.com/lookup?id=%s' % appId
		r = requests.get(url)
		if r.status_code != 200:
			logging.error("Requests to %s failed: %s", (url, r.text))
			return False

		resDict = json.loads(r.text)
		results = resDict['results']
		if len(results) != 1:
			logger.error("No app with id %s found", (appId))
			return False
		return self.schedule_bundleId(results[0]['bundleId'], account=account, country=country, executionStrategy=None)



	def schedule_itunes(self, url, account=None, country=None, executionStrategy=None):
		logger.info('Adding apps from iTunes (%s)' % url)
		r = requests.get(url)
		if r.status_code != 200:
			logging.error("Requests to %s failed: %s", (url, r.text))
			return False

		resDict = json.loads(r.text)
		entries = resDict['feed']['entry']
		result = True
		for entry in entries:
			if not self.schedule_bundleId(entry['id']['attributes']['im:bundleId'], account=account, country=country, executionStrategy=None):
				result = False
		return result
	
	def schedule_dioscope_new(self, host, index, account=None, country=None, executionStrategy=None):
		es = Elasticsearch([host])
		result = es.search(index=index, doc_type="app", fields="bundleId.untouched,currentVersionReleaseDate,version", size=1000)
		apps = result['hits']['hits']
		count = 0
		result = True
		for app in apps:
			fields = app['fields']
			bundleId = fields['bundleId.untouched'][0]
			oldVersion = fields['version'][0]
			oldVersionReleaseDate = dateutil.parser.parse(fields['currentVersionReleaseDate'][0])
			url = 'http://itunes.apple.com/lookup?bundleId=%s&country=%s' % (bundleId, country)
			r = requests.get(url)
			if r.status_code != 200:
				logging.error("Requests to %s failed: %s", url, r.text)
				return False
			resDict = json.loads(r.text)
			if 'results' not in resDict or len(resDict['results']) == 0:
				logging.error("%s not found in App Store", bundleId)
				continue
			
			newVersion = resDict['results'][0]['version']
			newVersionReleaseDate = dateutil.parser.parse(resDict['results'][0]['currentVersionReleaseDate'])

			if newVersionReleaseDate > oldVersionReleaseDate:
				count += 1

				logger.debug("%s (%s -> %s)", bundleId, oldVersion, newVersion)
				if not self.schedule_bundleId(bundleId, account=account, country=country, executionStrategy=executionStrategy, type='dioscope'):
					result = False
		
		logger.info("%d jobs scheduled", count)
		return result



def main():
	import argparse

	parser = argparse.ArgumentParser(description='schedule backend jobs from different sources.')
	parser.add_argument('-b','--backend', required=True, help='the backend url.')
	parser.add_argument('-a','--account', help='the accountId to use.')
	
	parser.add_argument('-s','--strategy', help='the execution strategy and duration to use.')
	
	# add commands
	cmdGroup = parser.add_argument_group('datasources', 'choose the datasource to take the app(s) from')
	mutalCmds = cmdGroup.add_mutually_exclusive_group(required=True)
	mutalCmds.add_argument('--bundleId', metavar='com.company.app', help='just schedule a given bundleId.')
	mutalCmds.add_argument('--appId', type=int, metavar='trackId', help='the apps appstore id')
	mutalCmds.add_argument('--itunes-top', type=int, default=10, nargs='?', metavar='n', help='use the top N free apps (defaults to 10)')
	mutalCmds.add_argument('--itunes-new', type=int, default=10, nargs='?', metavar='n', help='use the top N new (and free) apps (defaults to 10)')
	mutalCmds.add_argument('--dioscope-new', action='store_true', help='schedule jobs for new versions from apps in Elasticsearch index (max 1000 apps)')
	cmdGroup.add_argument('--itunes-genre', type=int, metavar='id', help='use the given genre only (defaults to all)')
	cmdGroup.add_argument('--itunes-country', type=str, default="de", nargs='?', metavar='countryCode', help='the store country to use (defaults to "de")')
	cmdGroup.add_argument('--dioscope-host', type=str, default="localhost", help='the Elasticsearch host for --dioscope-new')
	cmdGroup.add_argument('--dioscope-index', type=str, default="dioscope", help='the Elasticsearch index for --dioscope-new')


	args = parser.parse_args()
#	logger.debug(args)

	scheduler = Scheduler(args.backend)

	def printRes(res):
		if res:
			logger.info('done!')
		else:
			logger.error('error occured (could be partially done)')

	if 'bundleId' in args and args.bundleId:
		res = scheduler.schedule_bundleId(args.bundleId, account=args.account, country=args.itunes_country, executionStrategy=args.strategy)
		printRes(res)
		return

	if 'appId' in args and args.appId:
		res = scheduler.schedule_appId(args.appId, account=args.account, country=args.itunes_country, executionStrategy=args.strategy)
		printRes(res)
		return

	if 'dioscope_new' in args and args.dioscope_new:
		res = scheduler.schedule_dioscope_new(args.dioscope_host, args.dioscope_index, account=args.account, country=args.itunes_country, executionStrategy=args.strategy)
		printRes(res)
    	return

	genre = ''
	if args.itunes_genre:
		genre = 'genre=%i' % args.itunes_genre

	if 'itunes_top' in args and args.itunes_top:
		url = 'https://itunes.apple.com/%s/rss/topfreeapplications/limit=%i/%s/json' % (args.itunes_country, args.itunes_top, genre)
		res = scheduler.schedule_itunes(url, account=args.account, country=args.itunes_country, executionStrategy=args.strategy)
		printRes(res)
		return

	if 'itunes_new' in args and args.itunes_new:
		url = 'https://itunes.apple.com/%s/rss/newfreeapplications/limit=%i/%s/json' % (args.itunes_country, args.itunes_new, genre)
		res = scheduler.schedule_itunes(url, account=args.account, country=args.itunes_country, executionStrategy=args.strategy)
		printRes(res)
		return


if __name__ == '__main__':
	main()
