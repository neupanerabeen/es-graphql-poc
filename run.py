from fastapi import FastAPI
from elasticsearch import Elasticsearch
import strawberry
from strawberry.fastapi import GraphQLRouter
import uvicorn
from pydantic import typing
from strawberry.types import Info
from typing import Optional

from dataclasses import dataclass

es = Elasticsearch("https://elastic:m3JLoAK9htyf4J41qbBE@localhost:9200", verify_certs=False)

# api = FastAPI()
_data = [
	{"name":"ram", "pwd":"rampwd", "age":29},
	{"name":"sam", "pwd":"sampwd", "age":30}
]

def add_init_data():
	counter = 1
	for item in _data:
		print(item)
		es.create(
			index="users",
			id=counter,
			document=item	
			# document={
			# 	"name":"ram",
			# 	"pwd":"rampwd"
			# }	
		)
		counter +=1



try:
	add_init_data()
except:
	pass

class ResponseDataBoilerPlate:
	def __init__(self, doc = {}):
		for item in doc:
			self.__setattr__(item, doc[item])

def get_users(fields = [], user_id = None):
	selected_fields = ["hits.hits._source.{}".format(i) for i in fields]
	_d =  es.search(index="users", filter_path = selected_fields)
	# print(_d)
	if len(_d.keys()) == 0:
		return []
	res = _d["hits"]["hits"]
	res = [i["_source"] for i in res]
	obj_res = []
	for _res in res:
		u = ResponseDataBoilerPlate(_res)
		obj_res.append(u)
	return obj_res


@strawberry.type
class Users:
	name : str
	pwd: str
	age: str


@strawberry.type
class Query:
	@strawberry.field
	def test(self) -> str:
		return "test"

	@strawberry.field
	def users(self, info: Info, user_id : Optional[int] = None) -> typing.List[Users]:
		fields =  [ i.name for i in info.selected_fields[0].selections]
		res = get_users(fields, user_id)
		return res


schema = strawberry.Schema(Query)


graphql_app = GraphQLRouter(schema)


app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

uvicorn.run(app)
