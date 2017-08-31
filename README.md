# rds-startstop-scheduler

Python daemon script to launch/stop RDS instances automatically.


# Add tags to RDS.

You can add "start_time" and "stop_time" tags to RDS instance.

![tags_on_rds_instances](https://github.com/megmogmog1965/rds-startstop-scheduler/raw/readme_imgs/rds_tags.png "RDS Instance Tags")

This script reads these tags to schedule launching/stopping RDS instances.


# How to run.

Required Python 2.7.

* [Python 2.7.13]

Install libs by [pip].

```
$ pip install boto3 python-daemon --upgrade
```

Then, run script.

```
$ python rds_startstop_scheduler.py --region "REGION" --aws-access-key-id "your-aws-access-key-id" --aws-secret-access-key "your-aws-secret-access-key"
```

You can also use ``~/.aws/credentials`` [awscli] settings.

```
$ python rds_startstop_scheduler.py --region us-east-1 --profile default
```


[pip]:https://pip.pypa.io/en/stable/installing/
[Python 2.7.13]:https://www.python.org/downloads/release/python-2713/
[awscli]:http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
