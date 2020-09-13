# ELK CloudNine Project

### Steps to create a clone instance of ELK

  - Install Elasticsearch 7.9.1
  - Install the custom version of Kibana 7.9.1 (by @Vivek)
  - Migrate all index data from Old ES instance to the new one.
  - Migrate all saved objects from Old kibana instance to the new one.
  - Enable security on Elasticsearch and create custom users from management console.
  - Install nginx and configure server TLD to proxy to kibana with necessary headers.

NOTE: You need to create different spaces and import the saved objects independently, the dashboard ids don't change hence the custom left sidebar items will work as expected.

### Steps to enable startup of services on boot

```sh
$ sudo systemctl kibana enable
$ sudo systemctl elasticsearch enable
$ sudo systemctl mongod enable
```
