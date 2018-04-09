This is a Query Engine implementing several join methods, 2-stage random optimiser and aggregation. 
The implementation of this project
1. The parser will parse the SQL query and return different vectors of operator
2. Initial Plan will create a random initial plan
3. Optimiser will optimise the initial plan based on cost of different results of different methods of certain operators such as Join(Page Nested Join / Block Nested Join / Merge Sort Join)
4. After deciding the final execution plan, it will execute the operators based on the order of the tree and return the result

Our modification 
1. Block Nested Join that implemented in BlockNestedJoin class, which read one block of tuples instead of one page from left table compared to Page Nested Join. It will decrease IO cost compared to Page Nested Join
2. Sort Merge Join that implemented in SortMerge class and SortMergeJoin class, which sort the two table and do comparison
3. Distinct which implemented in Distinct class, this class will make the query return distinct tuples regarding the value of attribute under distinct
4. We implement new optimiser, which is 2-stage randomisation. The first stage is normal randomisation algorithm and second stage is annealing algorithm
5. GroupBy operator implemented in GroupBy class
6. Aggregation methods implemented in Aggregation class and GroupBy class 

