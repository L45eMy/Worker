This folder contains the SSH keys to access the analysis devices via SSH. Dioscope jobs require the Worker to access the devices via SSH without entering via keyfile.

The name of a key file must be the device's udid.

#### Generating Keys

* `cd` in this folder
* `ssh-keygen -f <device_udid> -N ""`
* Add the contents of `<device_udid>.pub` in the `~/.ssh/authorized_keys` file on the device. Create file and folders as appropriate.

#### Test Keys

* `cd` in this folder
* Run `ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i <device_udid> root@<device_ip>`
* You should be logged in without entering a password