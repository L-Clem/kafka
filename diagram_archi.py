from diagrams import Cluster, Diagram, Edge
from diagrams.aws.mobile import Amplify
from diagrams.aws.analytics import KinesisDataStreams
from diagrams.aws.analytics import KinesisDataFirehose
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3

with Diagram("Architecture Cloud Big Data", outformat="png", filename="Atelier 4 - Architecture Cloud Big Data", show=False):
    amp = Amplify("Amplify") 
    kds = KinesisDataStreams("Kinesis Data Streams") 
    kfh = KinesisDataFirehose("Kinesis Data Firehose") 
    lam = Lambda("Lambda Kinesis FH data processing")
    sss = S3("S3 Data Lake")

    amp >> kds >> kfh
    kfh - Edge(style="dashed") - lam
    kfh >> sss