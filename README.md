monasca-events-engine
=====================

The monasca event and stream processing engine (uses stacktach-winchester).
The event engine reads distilled events from the Kafka event topic and adds the
distilled events and temporary streams that match stream definitions
(filters, group-by, fire/expire criteria, fire/expire handlers) to the Mysql DB.
When stream fire criteria has been met, the stream of events is sent to the 
specified handler.  Stream definitions can be specified per tenant.

Under Development

# Installation

## Get the Code

```
git clone https://github.com/hpcloud-mon/monasca-events-engine
```

Requires:
  - winchester
      https://github.com/stackforge/stacktach-winchester
      https://github.com/oneilcin/stacktach-winchester (fork with dynamic changes is needed till we merge)
  - mysqlclient
  - mysql winchester DB - currently exists in monasca-vagrant

## Run it      
```
cd monasca_event
python main.py ../etc/monasca_event.conf 
```
