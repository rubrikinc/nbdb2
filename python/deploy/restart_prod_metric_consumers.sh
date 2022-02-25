#!/bin/bash -eux

echo "Stopping Influx & Graphite metric consumers"
python3.7 deploy_aws.py --target=master_graphite_consumer \
	--consumer_mode=realtime --state=down
python3.7 deploy_aws.py --target=master_graphite_consumer \
	--consumer_mode=rollup --state=down
python3.7 deploy_aws.py --target=prod_no_agg_graphite_consumer \
	--consumer_mode=realtime --state=down
python3.7 deploy_aws.py --target=prod_no_agg_graphite_consumer \
	--consumer_mode=rollup --state=down
python3.7 deploy_aws.py --target=prod_graphite_consumer \
	--consumer_mode=realtime --state=down
python3.7 deploy_aws.py --target=prod_graphite_consumer \
	--consumer_mode=rollup --state=down
python3.7 deploy_aws.py --target=v4_live_influx_consumer \
	--consumer_mode=realtime --state=down
python3.7 deploy_aws.py --target=v4_live_influx_consumer \
	--consumer_mode=rollup --state=down
python3.7 deploy_aws.py --target=internal_graphite_consumer \
  	--consumer_mode=realtime --state=down
python3.7 deploy_aws.py --target=internal_graphite_consumer \
  	--consumer_mode=rollup --state=down
python3.7 deploy_aws.py --target=prod_pickle_graphite_consumer \
  	--consumer_mode=realtime --state=down
python3.7 deploy_aws.py --target=prod_pickle_graphite_consumer \
  	--consumer_mode=rollup --state=down
python3.7 deploy_aws.py --target=v2_live_pickle_graphite_consumer \
  	--consumer_mode=realtime --state=down
python3.7 deploy_aws.py --target=v2_live_pickle_graphite_consumer \
  	--consumer_mode=rollup --state=down

echo "Waiting for 60 secs to let ECS services drain"
echo "-----------------------------------------------------------"
for t in {1..6}; do echo "Sleeping ${t}0/60 seconds" && sleep 10; done

echo "Starting Influx & Graphite metric consumers"
# Numbers copied from start_anomalydb_druid_prod.sh
python3.7 deploy_aws.py --target=master_graphite_consumer \
	--consumer_mode=realtime --state=up --num_instances=1
python3.7 deploy_aws.py --target=master_graphite_consumer \
	--consumer_mode=rollup --state=up --num_instances=1
python3.7 deploy_aws.py --target=prod_no_agg_graphite_consumer \
	--consumer_mode=realtime --state=up --num_instances=50
python3.7 deploy_aws.py --target=prod_no_agg_graphite_consumer \
	--consumer_mode=rollup --state=up --num_instances=50
python3.7 deploy_aws.py --target=prod_graphite_consumer \
	--consumer_mode=realtime --state=up --num_instances=1
python3.7 deploy_aws.py --target=prod_graphite_consumer \
	--consumer_mode=rollup --state=up --num_instances=1
python3.7 deploy_aws.py --target=v4_live_influx_consumer \
	--consumer_mode=realtime --state=up --num_instances=2
python3.7 deploy_aws.py --target=v4_live_influx_consumer \
	--consumer_mode=rollup --state=up --num_instances=2
python3.7 deploy_aws.py --target=internal_graphite_consumer \
  	--consumer_mode=realtime --state=up --num_instances=1
python3.7 deploy_aws.py --target=internal_graphite_consumer \
  	--consumer_mode=rollup --state=up --num_instances=1
python3.7 deploy_aws.py --target=prod_pickle_graphite_consumer \
  	--consumer_mode=realtime --state=up --num_instances=1
python3.7 deploy_aws.py --target=prod_pickle_graphite_consumer \
  	--consumer_mode=rollup --state=up --num_instances=1
python3.7 deploy_aws.py --target=v2_live_pickle_graphite_consumer \
  	--consumer_mode=realtime --state=up --num_instances=1
python3.7 deploy_aws.py --target=v2_live_pickle_graphite_consumer \
  	--consumer_mode=rollup --state=up --num_instances=1
