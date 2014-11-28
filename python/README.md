To build the library:
pip install -r src/requirements.txt -t src/

For use with VPM:
```javascript
{

    "name": "mapreduce",
    "githubPath": "appengine-mapreduce/python/src/mapreduce",
    "installPath": "src/lib/mapreduce"
},
{
    "name": "mapreduce",
    "githubPath": "appengine-mapreduce/python/src/graphy",
    "installPath": "src/lib/graphy"
},
{
    "name": "mapreduce",
    "githubPath": "appengine-mapreduce/python/src/cloudstorage",
    "installPath": "src/lib/cloudstorage"
},
{
    "name": "mapreduce",
    "githubPath": "appengine-mapreduce/python/src/pipeline",
    "installPath": "src/lib/pipeline"
},
{
    "name": "mapreduce",
    "githubPath": "appengine-mapreduce/python/src/simplejson",
    "installPath": "src/lib/simplejson"
}
```
