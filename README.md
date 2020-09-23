# ELK CloudNine Project

### Steps to create a clone instance of ELK

  - Install Elasticsearch 7.9.1
  - Install the custom version of Kibana 7.9.1 (by @Vivek)
  - Migrate all index data from Old ES instance to the new one.
  - Migrate all saved objects from Old kibana instance to the new one.
  - Enable security on Elasticsearch and create custom users from management console.
  - Install nginx and configure server TLD to proxy to kibana with necessary headers.

NOTE: You need to create different spaces and import the saved objects independently, the dashboard ids don't change hence the custom left sidebar items will work as expected.

### Notes while migrating kibana instances across cloud providers
- You need to create different spaces and import the saved objects independently, the dashboard ids don't change hence the custom left sidebar items will work as expected.
- The users aren't migrated as part of the saved objects migration process. They get deleted when you deleted meta indexes in Elasticsearch (.kibana, .security, etc.)
- The meta indexes are ought to be deleted else it would result in inconsistent state with uuids being different from that of the cluster state.
- Make sure to change the `elasticsearch.username` and `elasticsearch.password` in kibana.yml whenever one changes it in Kibana using the security settings.

### Steps to enable startup of services on boot

```sh
$ sudo systemctl kibana enable
$ sudo systemctl elasticsearch enable
$ sudo systemctl mongod enable
```
