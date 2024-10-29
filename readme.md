# Sample Stream Processing ⚙️

Sample playground-1.mongodb.js for this functionality : 
- It uppercase the name
- Change the M to Male and F to Female
- Validate the city name; if it’s wrong, it will go to the dead letter queue

**SP Command**
- List all the stream processor
```bash
sp.listStreamProcessors()
```
- Get verbose statistic 
```bash
sp.Stream2.stats({options:{verbose : true}})
```
- Set the worker number 
```bash
db.runCommand({startStreamProcessor:"Stream1", workers:1})
```