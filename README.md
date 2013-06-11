elasticsearch-storedProcedure
=============================

an elasticsearch plugin, with this plugin, you are able to save cumbersome search/update payload, give it a name, and then call the name with a "GET" call.

there are two different stored procedure for now:
*stored search (_search)
*stored update (_update)

#Version
only tested with elasticsearch-0.19.11, more version will be added soon.

# Usage
* define a stored search:
> curl -XPUT http://localhost:9200/index/type/_storedprocedure/testSearch -d '
> {
>    "query":{
>    	"match":{"category":"${category}"}
>    }
>}
>'

* check it:
> curl -XGET http://localhost:9200/index/type/_storedprocedure/testSearch

* delete it:
> curl -XDELETE http://localhost:9200/index/type/_storedprocedure/testSearch

execute it:
> curl -XGET http://localhost:9200/index/type/_storedprocedure/testSearch?op=run&p=category:books

same as the stored update:
> curl -XPUT http://localhost:9200/index/type/_storedprocedure/testUpdate -d '
> {
>     "script" : "ctx._source.count += count",
>     "params" : {
>         "count" : ${count}
>     }
> }
> '

you can leverage freemarker for further usage.

