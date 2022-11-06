'''
    mongodb interface for storing, caching and retrieving caches
    assumes tha mongodb is installed
'''

import sys
import pymongo

class DBManager:
    __PORT__ = 27017
    __SCHEMA__ = {
        'bsonType': 'object',
        'additionalProperties': True,
        'required': ['name', 'url', 'reviews'],
        'properties': {
            'name': {
                'bsonType': 'string'
            },
            'url': {
                'bsonType': 'string'
            },
            "page": {
                'bsonType': 'int'
            },
            'place_id': {
                'bsonType': 'string'
            },
            'geometry': {
                'bsonType': 'object',
                'properties': {
                    "lat": {
                        'bsonType': 'double'
                    },
                    "lng": {
                        'bsonType': 'double'
                    }
                }
            },
            "reviews": {
                'bsonType': 'array',
                'items': {
                    'bsonType': 'object',
                    'properties': {
                        'metadata': {
                            'bsonType': 'object',
                            'required': ['title', 'text', 'month', 'year'],
                            'properties': {
                                'rating': {
                                    'bsonType': 'int'
                                },
                                'title': {
                                    'bsonType': 'string'
                                },
                                "text": {
                                    'bsonType': 'string'
                                },
                                'date': {
                                    'bsonType': 'int'
                                },
                                "month": {
                                    'bsonType': 'string'
                                },
                                "year": {
                                    'bsonType': 'int'
                                }
                            }
                        },
                        'images': {
                            'bsonType': 'array',
                            'items': {
                                'bsonType': 'string'
                            }
                        }
                    }
                }
            }
        }
    }

    def __init__(self, dbName, tableName, logging):
        self.dbName = dbName
        self.tableName = tableName
        self.logging = logging
        
        try:
            client = pymongo.MongoClient("localhost", self.__PORT__)
            self.db = client[self.dbName]
            self.__create_collection__(collection_name=self.tableName)
            self.collection = self.db[self.tableName]
            self.collection.create_index('url', unique=True)
        except:
            self.logging.error("mongodb error", exc_info=True)
            sys.exit()

        

    def __create_collection__(self, collection_name='reviews'):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(
                collection_name,
                validator = {
                    '$jsonSchema': self.__SCHEMA__
                }
            )

    def insert(self, docs):
        if len(docs) == 0:
            return
        
        if len(docs) == 1:
            try:
                self.collection.insert_one(docs[0])
            except:
                self.logging.warning("error inserting document.", exc_info=True)
        else:
            '''
                insert_many with ordered=false tries to insert each of the documents
                individually, preferably in parallel. unlike ordered=true, it does not 
                insert in order. hence, if one insertion fails, the subsequent ones don't.

                11000 - duplicate error code
            '''
            try:
                self.collection.insert_many(docs, ordered=False)
            except pymongo.errors.BulkWriteError as ex:
                errors = ex.details['writeErrors']
                duplicateErrors = list(filter(lambda x: x['code'] == 11000, errors))
                errors = list(filter(lambda x: x['code'] != 11000, errors))

                if len(errors) > 0:
                    self.logging.error("error inserting document(/s).", exc_info=True)
                else:
                    self.logging.info(f"{len(duplicateErrors)} duplicates found. successfully inserted {len(docs) - len(errors) - len(duplicateErrors)} documents.")
            except Exception as ex:
                self.logging.error("error inserting documents.", exc_info=True)
        

    def query(self, val, col="url"):
        try:
            cursor = self.collection.find({col: val})
            docs = list(cursor)

            print(docs)

            if len(docs) > 0:
                return docs[0]
            else:
                return None
        except:
            self.logging.error("error querying for document(/s).", exc_info=True)
            return None
