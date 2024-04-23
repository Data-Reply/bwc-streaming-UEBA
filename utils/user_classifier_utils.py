import os
import pandas as pd
import numpy as np
import logging

from sklearn.preprocessing import OneHotEncoder ##encoder of user_id column
from sklearn.cluster import KMeans ##clutering scheme

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer


logging.basicConfig(filename='streaming-UEBA.log', level=logging.INFO)
transactions_aggregate_content = {}
    
class User_labeling(object):
    """
    User_labeling record

    Args:
        user_id (str): User's id
        total_amount (double): User's amount
        num_transactions (int): User's number transactions
        label: User's label (for example 0=stingy, 1=standard, 2=spendthrift)
    """

    def __init__(self, user_id, total_amount, num_transactions, label):
        self.user_id = user_id
        self.total_amount = total_amount
        self.num_transactions = num_transactions
        self.label = label

class User(object):
    """
    User record

    Args:
        user_id (str): User's id
        total_amount (double): User's amount
        num_transactions (int): User's number transactions
    """
    def __init__(self, user_id=None, total_amount=None, num_transactions=None):
        self.user_id = user_id
        self.total_amount = total_amount
        self.num_transactions = num_transactions


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        logging.info("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    logging.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def user_to_dict(user, ctx):
    """
    Converts a User instance to object literal(dict).

    Args:
        user: user istance
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """
    return dict(user_id=user.user_id,
                total_amount=user.total_amount,
                num_transactions=user.num_transactions,
                label=user.label)

def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    return User(user_id=obj['user_id'],
                total_amount=obj['total_amount'],
                num_transactions=obj['num_transactions']
                )
    

def consumer(args, sr_conf, consumer_conf, producer_conf):
    
    topic = args.topic
    schema_registry_client = SchemaRegistryClient(sr_conf)
    schema_str = schema_registry_client.get_latest_version(f"{topic}-value").schema.schema_str
    logging.info(schema_str)
    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    # consume the transactions
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                logging.info("Waiting...")
                continue
            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))                       
            if user is not None:
                logging.info(f"user record key = {msg.key()}:")
                logging.info(f"user_id = {user.user_id}")
                logging.info(f"total_amount = {user.total_amount}")
                logging.info(f"num_transactions = {user.num_transactions}\n")             

                u = [user.total_amount, user.num_transactions]
                transactions_aggregate_content[user.user_id] = u

                clustering(transactions_aggregate_content, schema_registry_client, producer_conf, args.n_cluster)   

        except KeyboardInterrupt:
            break

    consumer.close()

# function to send the user labeling output to user_labeling topic
def producer(data_clustering, schema_registry_client, producer_conf):
    
    topic ="user_labeling"
    schema_str = schema_registry_client.get_latest_version(f"{topic}-value").schema.schema_str
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    producer = Producer(producer_conf)

    for _, row in data_clustering.iterrows():
        try:
            # create an instance of specific user
            user = User_labeling(user_id=row["user_id"],
                        total_amount=row["total_amount"],
                        num_transactions=row["num_transactions"],
                        label=row["label"])
            
            # send the classification of user to user_labeling topic
            producer.produce(topic=topic,
                             key=row["user_id"],
                             value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            logging.info("Invalid input, discarding record...")

    logging.info("\nFlushing records...")
    producer.flush()

def clustering(dictionary, schema_registry_client, producer_conf, n_cluster):
    # ricreate a pandas dataframe from dictionary
    data_clustering = pd.DataFrame(columns=["user_id", "total_amount", "num_transactions"])
    for index, us in enumerate(dictionary):
        nested_list = [us, dictionary[us]]
        flattened_list = [item for sublist in nested_list for item in (sublist if isinstance(sublist, list) else [sublist])]
        data_clustering.loc[index] = flattened_list
    
    # this check is necessary because in our case, if the number of users is less than 3, the clustering algorithm is irrelevant
    if data_clustering.shape[0] < 3:
        data_clustering["label"] = np.zeros(len(data_clustering), dtype=int).tolist()
    else:    
        X = data_clustering

        # istantiate the encoder to parse the user_id column (user_id is a string and Kmeans accept only numerical data as input)
        onehot_encoder = OneHotEncoder(sparse_output = False)
        
        # fit and transform the column user_id
        X_encoded = onehot_encoder.fit_transform(X["user_id"].array.reshape(-1, 1))

        # create a temp dataframe with only information about user_id column
        df_encoded = pd.DataFrame(X_encoded, columns=onehot_encoder.get_feature_names_out())
        
        # merge the correct parsing of user_id column
        X = X.drop("user_id", axis = 1).join(df_encoded)
        
        k_optimal = n_cluster
        
        ##istantiate the best kmeans clustering scheme
        km = KMeans(n_clusters=k_optimal, init='k-means++', n_init='auto', random_state=42) ##random state to ensure the reproducibility of experiment

        ##compute cluster centers and predict cluster index for each sample
        y_km = km.fit_predict(X)

        ##add a columns y_km to dataframe
        data_clustering["label"] = y_km

    producer(data_clustering, schema_registry_client, producer_conf)
