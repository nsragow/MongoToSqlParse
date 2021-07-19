# Interview Take Home Test
#### Convert MongoDB `find()` to SQL `SELECT`
Author: Noah Sragow
### Usage
command: `python Main.py 'db.user.find({})'`  
output: `SELECT * FROM user;`  
__OR__  
`from Main import mongo_to_sql`

### Test
` python -m unittest discover`
### Supported Operations
* db.collection.find()
  * query
    * `or`
    * `and` (including implicit `and` from the comma of a compound query)
    * `lt`
    * `lte`
    * `gt`
    * `gte`
    * `ne`
    * `in`

  * projection
    * field inclusion with 1 or true
      > Exclusion would require a list of known columns.
  * types
    * string
    * numeric
    * list
    * boolean
    * null
    
### Hurdles
* order of operations
  * This can be a problem when considering `and` and `or`. Proper use of `()` is required.
* typing
  * How will the parser handle the difference between `1` and `"1"`
* bad input
  * For now I will not handle this. But it is possible to pass a poorly formatted `find`
* MongoDB arguments
  * How does mongo handle one, both or none of the arguments?
* Regex?
* SQL double vs single quotes


### Tools used
https://www.pdbmbook.com/playground to verify MongoDB syntax


### Decisions
* Recursive parsing
  * MongoDB query syntax is recursively defined. 
* Overuse of parenthesis.
  * There are scenarios where the used parenthesis may be unnecessary. They are used anyways to reduce dev time needed to comply with order of operations. This reduces the readability of the SQL output.
* Sorta state machine
  * A pure state machine would have been the most efficient way to solve this. Most of the functionality follows this mindset although lookbacks are used to extract text and some functions (like the list parser) go through the same text again. This is done to improve the readability of the tools src considering mongo find query parsing should not be a runtime intensive operation. 
  
### Edge Cases
* Strangely Mongo will accept duplicate keys in a dictionary. It overrides the earlier value with later values in the online testing tool that I used.
  > { type:{$ne: "rose"}, type: "rose"} returns {type: "rose"}

### Notes to grader
I have very little MongoDB experience. As such, I covered the obvious reach of the syntax I found online.  
There may be syntax edge cases that I did not cover. For example, if MongoDB excepts a semicolon at the end of a query that will break the JSON parsing.  
I am not seeking to cover these possible syntax edge cases as I do not feel that the test asks for this type of knowledge and the time to fully familiarize myself with all possible syntax permutations is out of scope.  
There are also features I am not supporting because based on my research it seems that they require operators not specified in the test doc. 
#### Unsupported features
* Field to field comparisons
  * seems to require `$where` and `$expr` operators
* Datetime/Timestamp
  * seems to require Date function
* Regex
  * I ran out of time. This may be as simple as passing the literal regex to SQL or it may be as complex as converting from one regex language to another.
