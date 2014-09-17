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
         
      python main.py [~/publicGithub/monasca-event/monasca_event.yaml]

  2. A tool to generate nova events, simulate the API, and write to kafka.
     
      cd monasca_event
      
      Requires: 
        - notigen  (clone it and sudo python setup.py install)
        - kafka-python
        - PyYaml
       
      python notigen_to_kafka.py '/Users/cindy/publicGithub/monasca-event/monasca_event.yaml' 50 1
      
      python notigen_to_kafka.py  (prints usage)

  3. The Paris prototype for the winchester event processing pipeline in monasca
     
      cd monasca_event
      
      TBD
