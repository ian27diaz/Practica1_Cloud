import boto3
from boto3.dynamodb.conditions import Key
from .config import Config

class DynamoStorage():
    def __init__(self, configPath):
        super().__init__()
        self.config = Config(configPath)
        # Get the service resource with the region.
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name = self.config.get_region()
        )
        #Create tables in case they aren't created
        #images table
        try:
            self.dynamodb.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'keyword',
                        'AttributeType': 'S'
                    }
                ],
                TableName='images',
                KeySchema=[
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'
                    }
                ],
                ProvisionedThroughput = {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
        except Exception as e:
            print("Exception ocurred creating images table: {}".format(e.__class__.__name__)) 
        else:
            print('Images table created')
        
        #label table
        try:
            self.dynamodb.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'keyword',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'inx',
                        'AttributeType': 'N'
                    }                    
                ],
                TableName='labels',
                KeySchema=[
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'inx',
                        'KeyType': 'RANGE'
                    }
                ],
                ProvisionedThroughput = {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
        except Exception as e:
            print("Exception ocurred creating labels table: {}".format(e.__class__.__name__)) 
        else:
            print('Labels table created')
        
        #Getting the tables from DynamoDB
        self.images_table = self.dynamodb.Table('images')
        self.labels_table = self.dynamodb.Table('labels')

    def putImage(self, category, url):
        try:
            print("Inserting ", category, "->", url)
            self.images_table.put_item(
                Item={
                    'keyword': category,
                    'url': url,
                }
            )
        except Exception as e:
            print("Exception ocurred adding item to images table: {}".format(e.__class__.__name__)) 
        else:
            print('Item added')

    def putLabel(self, label, sort, category):
        try:
            print("Inserting (", label,",", sort, ") ->", category)
            self.labels_table.put_item(
                Item = {
                    'keyword': label,
                    'inx': int(sort),
                    'category': category
                }
            )
        except Exception as e:
            print("Exception ocurred adding item to labels table: {}".format(e.__class__.__name__)) 
        else:
            print('Item added')
    
    def put_images(self, imagesDictionary):
        reqs = []
        req = {"images": []}
        i = 0
        for img_key in imagesDictionary.keys():
            put_req = {
                'PutRequest': {
                    'Item': {
                        'category': img_key,
                        'url': imagesDictionary[img_key]
                    }
                }
            }
            i += 1
            req['images'].append(put_req)
            # batch_write_item() only supports 25 items per request.
            if (i >= 25):
                reqs.append(req)
                req = {"images": []}
                i = 0

        if (len(req['images']) > 0):
            reqs.append(req)

        for request in reqs:
            self.dynamodb.batch_write_item(RequestItems = request)
        return

    def queryImageByLabel(self, keyword):
        res = self.labels_table.query(KeyConditionExpression=Key('keyword').eq(keyword))
        categories = []
        matches = []
        for item in res['Items']:
            categories.append(item['category'])
        
        for cat in categories:
            r = self.images_table.get_item(
                Key = {'keyword': cat},
                AttributesToGet=['url'],
            )
            matches.append(r['Item']['url'])
        return matches
