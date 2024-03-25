import json
from pymongo import MongoClient


def regions():
    with open("Data\\data.json", "r") as f:
        data = json.load(f)
    return list(data.keys())


class DatabaseManager:
    def __init__(self):
        self.client = MongoClient("localhost", 27017)
        self.database = self.client["Database"]

    def create_table(self):
        pass

    def add_data(self, table_name, **kwargs):
        self.database[table_name].insert_one(kwargs)

    def get_existing_relations(self):
        result = self.database["student_advisor"].find()
        return [(i['student_id'], i['advisor_id'],) for i in result]

    def delete_row(self, table_name, row_id):
        if table_name == "advisor":
            self.database[table_name].delete_one({"advisor_id": row_id})
        else:
            self.database[table_name].delete_one({"student_id": row_id})

    def load_data(self, table_name):
        query = self.database[table_name].find({}, {"_id": 0} if table_name == "student_advisor" else {})
        return [tuple(line.values()) for line in query]

    def search(self, table_name, **kwargs):
        if len(kwargs) == 0:
            query = self.database[table_name].find({}, {"_id": 0} if table_name == "student_advisor" else {})
        else:
            query = self.database[table_name].find(kwargs, {i: 1 for i in kwargs if kwargs[i] is not None})
        return [tuple(line.values()) for line in query]

    def update(self, table_name, name, surname, age, id):
        self.database[table_name].update_many({"_id": id}, {"$set": {"name": name, "surname": surname, "age": age}})

    def check_bd(self):
        return True if not list(self.database["student_advisor"].find()) else False

    def list_advisors_with_students_count(self, order_by):
        query = self.database["advisors"].aggregate([
            {
                "$lookup": {
                    "from": "student_advisors",
                    "localField": "_id",
                    "foreignField": "advisor_id",
                    "as": "students"
                }
            },
            {
                "$addFields": {
                    "num_students": {"$size": "$students"}
                }
            },
            {
                "$sort": {"num_students": 1 if order_by == "ASC" else -1}
            }
        ])

        return [(i["_id"], i["name"], i["surname"], i["num_students"]) for i in query]

    def list_students_with_advisors_count(self, order_by):
        query = self.database["advisors"].aggregate([
            {
                "$lookup": {
                    "from": "student_advisors",
                    "localField": "_id",
                    "foreignField": "student_id",
                    "as": "advisors"
                }
            },
            {
                "$addFields": {
                    "num_advisors": {"$size": "$advisors"}
                }
            },
            {
                "$sort": {"num_advisors": 1 if order_by == "ASC" else -1}
            }
        ])

        return [(i["_id"], i["name"], i["surname"], i["num_advisors"]) for i in query]
