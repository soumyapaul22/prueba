import json
import boto3
import datetime

def lambda_handler(event, context):
    print("Creating EMR")
    connection = boto3.client('emr', region_name='us-east-1')
    print(event)
    cluster_id = connection.run_job_flow(
        Name='Emr_Spark',
        LogUri='s3://aws-logs-251322036489-us-east-1/elasticmapreduce/logs',
        ReleaseLabel='emr-5.21.0',
        Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Spark'}],
        BootstrapActions= [
        {
            "Name": "BootstrapAction1",
            "ScriptBootstrapAction": {
                "Path": "s3://emr-spark-dependencies/dependencies.sh"
            }
        }
    ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 400
                                },
                                'VolumesPerInstance': 2
                            },
                        ],

                        'EbsOptimized': True,
                    }
                },
                {
                    'Name': 'Slave nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 500
                                },
                                'VolumesPerInstance': 2
                            },
                        ],
                        'EbsOptimized': True,
                    },
                    'AutoScalingPolicy': {
                        'Constraints': {
                            'MinCapacity': 2,
                            'MaxCapacity': 20
                        },
                        'Rules': [
                            {
                                'Name': 'Compute-scale-up',
                                'Description': 'scale up on YARNMemory',
                                'Action': {
                                    'SimpleScalingPolicyConfiguration': {
                                        'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                        'ScalingAdjustment': 1,
                                        'CoolDown': 300
                                    }

                                },
                                'Trigger': {
                                    'CloudWatchAlarmDefinition': {
                                        'ComparisonOperator': 'LESS_THAN',
                                        'EvaluationPeriods': 120,
                                        'MetricName': 'YARNMemoryAvailablePercentage',
                                        'Namespace': 'AWS/ElasticMapReduce',
                                        'Period': 300,
                                        'Statistic': 'AVERAGE',
                                        'Threshold': 20,
                                        'Unit': 'PERCENT'
                                    }
                                }
                            },
                            {
                                'Name': 'Compute-scale-down',
                                'Description': 'scale down on YARNMemory',
                                'Action': {
                                    'SimpleScalingPolicyConfiguration': {
                                        'AdjustmentType': 'CHANGE_IN_CAPACITY',
                                        'ScalingAdjustment': -1,
                                        'CoolDown': 300
                                    }
                                },
                                'Trigger': {
                                    'CloudWatchAlarmDefinition': {
                                        'ComparisonOperator': 'LESS_THAN',
                                        'EvaluationPeriods': 125,
                                        'MetricName': 'YARNMemoryAvailablePercentage',
                                        'Namespace': 'AWS/ElasticMapReduce',
                                        'Period': 250,
                                        'Statistic': 'AVERAGE',
                                        'Threshold': 85,
                                        'Unit': 'PERCENT'
                                    }
                                }
                            }
                        ]
                    }
                }
            ],

            'KeepJobFlowAliveWhenNoSteps': False,
            'Ec2KeyName': 'ubuntu_key',
            'Ec2SubnetId': 'subnet-ca97a1e4',
            'EmrManagedMasterSecurityGroup': 'sg-05587e92763b37349',
            'EmrManagedSlaveSecurityGroup': 'sg-0dd40ac58cb74f10f'
        },
        Steps=[
            {
                'Name': 'spark-submit',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit' , '--deploy-mode', 'client' , '--jars', 's3://emr-spark-dependencies/postgresql-42.2.11.jar' , '--py-files', 's3://data-prueba-tecnica/prueba_lib.py' , 's3://data-prueba-tecnica/pruebaMain.py' , 'data-prueba-tecnica' , 'data_prueba_tecnica.csv'
                    ]
                }
            }
        ],
        AutoScalingRole='EMR_AutoScaling_DefaultRole',
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        EbsRootVolumeSize=100,
        Tags=[
            {
                'Key': 'NAME',
                'Value': 'Emr_Spark',
            },
        ]
    )
    print(cluster_id)