import requests
import time
import sys
import os
import json

## [resource_id, profile_id, permission_level_id, permission_type_id]
grant_list = [
  [252806188692978,248054054525066,246530842202790,246996234271275],
  [254596386033514,248054054525066,246530842106489,246996234271275],
  [254596386033514,248054151105372,246530842202790,246996234271275],
  [255298784802397,248929325392547,246530842202790,246996234271275],
  [255298784802397,251313350199390,246530842106489,246996234271275],
  [255907968592995,248690997319455,246530842106489,246996234271275],
  [255907968592995,248691137610104,246530842106489,246996234271275],
  [255907968592995,251494940672290,246530842137224,246996234271275]
]


class GraphQlException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class GraphQl:
    def __init__(self, endpoint: str, access_key: str, secret_access_key: str) -> None:
        if not endpoint or type(endpoint) is not str:
            raise ValueError("endpoint is missing or not string type")

        if not access_key or type(access_key) is not str:
            raise ValueError("access_key is missing or not string type")

        if not secret_access_key or type(secret_access_key) is not str:
            raise ValueError("secret_access_key is missing or not string type")

        self.__endpoint = endpoint
        self.__access_key = access_key
        self.__secret_access_key = secret_access_key

    def get_endpoint(self) -> str:
        return self.__endpoint

    def get_access_key(self) -> str:
        return self.__access_key

    def get_secret_access_key(self) -> str:
        return self.__secret_access_key

    def run_query(self, query: str, variables: dict) -> dict:
        if not query or type(query) is not str:
            raise ValueError("query is missing or not string type")

        if not variables or type(variables) is not dict:
            raise ValueError("variables is missing or not dict type")

        print(f"Query: {query}")
        print(f"Variables: {variables}")

        response = requests.post(
            self.get_endpoint(),
            auth=(self.get_access_key(), self.get_secret_access_key()),
            json={'query': query, 'variables': variables}
        )

        return response


def get_activate_grant_mutation():
  return '''
    mutation ActivateGrant($input1: ActivateGrantInput!) {
      activateGrant1: activateGrant(input: $input1) {
        turbot {
          id
          __typename
        }
        __typename
      }
    }
'''

def get_activate_grant_variables(grant_id, resource_id):
  return {
    "input1": {
      "grant": grant_id,
      "resource": str(resource_id)
    }
  }

def get_create_grant_mutation():
  return '''
    mutation CreateGrants($input1: CreateGrantInput!) {
    createGrant1: createGrant(input: $input1) {
      turbot {
        id
        __typename
      }
      __typename
    }
  }
'''

def get_create_grant_variables(resource_id, profile_id, permission_level, permission_type):
  return {
    "input1": {
      "type": str(permission_type),
      "level": str(permission_level),
      "resource": str(resource_id),
      "roleName": None,
      "validToTimestamp": None,
      "identity": str(profile_id)
    }
  }

graphql_endpoint = "https://console.prod.amazon.biogen.com/api/v5/graphql"
turbot_access_key = "e90202aa-a152-452b-96e4-f84d57c2a2d8"
turbot_secret_access_key = "b4f4bd85-bd78-421e-88ad-69921769e8f5"
graph_ql = GraphQl(graphql_endpoint, turbot_access_key, turbot_secret_access_key)
create_grant_mutation = get_create_grant_mutation()
activate_grant_mutation = get_activate_grant_mutation()

max = len(grant_list)
start_location = 0
batch_size = 10
batches = 1
wait = 20

for b in range(batches):
  print(f"Starting Batch {b}")
  start = start_location + (b * batch_size)
  end = start + batch_size
  print (f"{start} < {max}?")
  if start < max:
    print (f"{end} > {max}?")
    if end > max:
      end = max
    print (f"{start},{end}")
    for i in range(start,end):
      print(f"Starting record: {i}")
      grant = grant_list[i]
      create_grant_vars = get_create_grant_variables(grant[0], grant[1], grant[2], grant[3])
      response = graph_ql.run_query(create_grant_mutation, create_grant_vars)

      if response.status_code != 200 or response.json().get("errors"):
        print("Create Error: {}".format(response.text))
        print("CreateErrData: {},{},{},{}".format(grant[0], grant[1], grant[2], grant[3]))
        continue

      response = response.json()

      grant_id = response.get('data').get('createGrant1').get('turbot').get('id')
      if grant_id:
        activate_grant_vars = get_activate_grant_variables(grant_id, grant[0])
        response = graph_ql.run_query(activate_grant_mutation, activate_grant_vars)
        if response.status_code != 200 or response.json().get("errors"):
          print("Activate Error: {}".format(response.text))
          print("ActvateErrData: GrantId:{} ResourceId:{}".format(grant_id, grant[0]))

  print(f"Pausing Execution for {wait} seconds")
  time.sleep(wait)
