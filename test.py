
from elasticsearch_dsl import Search
from pyunravel import ES

es = ES()



search = Search(using=es.es, index="cloud_aws-*")
search.update_from_dict({
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "type": ["instanceFleets","instanceGroups"]
                                },
                            }
                        ]
                    }
                }
})
search.scan()
print(search.to_dict())
#search.query = Q('bool', must=[Q('match', kind='spark')])

#search.query = Q('bool', must=[Q('terms', eventName=self.events)])
for row in search.scan():
    row_dict = row.to_dict()
    print(row_dict)
