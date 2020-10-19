Mapper and Reducer:
* Our code is a reducer join.
* It is comparatively simple and easier to implement than the map side join as the sorting and shuffling phase sends the values having identical keys to the same reducer and therefore, by default, the data is organized for us.
* Mapper method is used to reformat the text value input from the input file. 
* Key value pairs are created for the given input file which will serve as input for reducer
* Now, reducer takes these key value pairs and joins them to the output file.
* In driver class, we create a job and then we pass reducer and mapper classes to job.
* Job waits until all the code execution is done
* Once the job is completed we go to Hadoop file system to view the output.
