monasca-event
=============

This repo contains 3 things:
  1. A prototype using the stacktach oahu pipeline processing libraries.
      
      cd monasca_event/oahu_test
      
      Requires (see requirements.txt): 
          - notigen
          - oahu
          - PyYaml
          - kazoo
         
      python main.py [~/publicGithub/monasca-event/etc/monasca_event.yaml]

  2. A tool to generate nova events, simulate the API, and write to kafka.
     
      cd monasca_event/utils
      
      Requires: 
        - notigen  (clone it and sudo python setup.py install)
        - kafka-python
        - PyYaml
       
      python notigen_to_kafka.py '/Users/cindy/publicGithub/monasca-event/etc/monasca_event.yaml' 50 1
      
      python notigen_to_kafka.py  (prints usage)

  3. The Paris prototype for the winchester event processing pipeline in monasca
    
      Based on winchester:
      https://github.com/stackforge/stacktach-winchester
 
      cd monasca_event
     
      install winchester

      make sure you have a mysqlclient installed

      mysql db needs: winchester DB, a user with access to this DB.
 
      python main.py '/Users/cindy/publicGithub/monasca-event/etc/monasca_event.yaml' 
