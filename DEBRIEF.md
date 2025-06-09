## Debrief

This debrief contains my recommendations for the improvement of the project. 

### Code quality / readability 

- Code would overall benefit from introducing TypeScript to better keep track of schemas, mapping without proper typing leads to weird bugs.
- Introducing some tests, either unit or integration, would force the code to be structured in a more testable and thus readable way.

### Project architecture

- Project should be split into modules with clear responsiblities. For example, stuff dealing with hubspot API goes into `hubspot.js` while stuff dealing with actions goes into `actions.js` 

### Code performance

Code performance is highly dependant on the cardinality of the entities we are dealing with. I usually work by figuring out where the bottleneck in the pipeline is and fixing/optimizing rather than doing any preoptimization. For the sake of the argument:

- If the cardinality is small as it is in this example, I would omit any concurrency and queuing. 
- Companies and Contacts are as optimal as they can be where we can probably adjust by playing around with page size (it's hardcoded to 100 in the code but it might be faster to get 1000 at a time for example)
- Meetings get more complex as they involve 2 more calls to the HubSpot API:
    - We can't avoid calling associations as the invite list might have been updated, so that one has to stay as it is
    - We can cache contact ID to email, again, depending on cardinality maybe without limits or some LRU cache, depends
- Currently batching on the database call to save actions is set to 2000, there's also room to play around with that parameter based on performance

Overall my recomendation would be to introduce some sort of instrumentation (Prometheus, OTEL) and figure out what the bottleneck is for a particular usecase. I imagine there's no one size fits all.

### Problems/bugs

- [X] LTE, GTE filter operators were mispelled (LTQ, GTQ) 
- [X] The way backoff was implemented had the hubspot API calls silently fail, so I moved it to backoff function and had each method call that instead
- [ ] I would prefer to keep last pull dates in reference to the source of truth rather than `new Date()`, so I would ideally keep it at the modified timestamp of the last data point retrieved. That potentially saves a lot of time chasing time zone problems and potential mismatches between hubspot and local machine time.
- [ ] Last pull date is used for contacts and companies to determine `isCreated` flag. However it is updated only when all batches are processed. It is possible that the same contact/company arrives in two different pages and you end up having two created actions for the same entity. So I would recomend that after each action is recorded this timestamp be udpated locally. That way we are only comparing the created timestamp with whatever was already processed. 
- [ ] Would be nice that at least for debugging logging goes to stderr and data is printed on stdout so that we can pipe into jq for faster debugs.
- [ ] `propertyPrefix` is unused
- [ ] `saveDomain` has unreachable code which looks ugly. These test runs can be achieved via flags or env vars.
- [ ] (Personal nitpick) I personally don't like keeping global variables like `expirationDate` or `hubspotClient` in this case. I prefer having those scoped to the method that uses them. It's rather dangerous that any method is able to override them as it can lead to weird bugs and long debugs.

