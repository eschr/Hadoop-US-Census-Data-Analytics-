src
├── pkg
│   ├── MainClass.java
│   ├── StateMapper.java
│   └── StateReducer.java
├── question9
│   ├── Q9Mapper1.java
│   ├── Q9Mapper2.java
│   ├── Q9Reducer1.java
│   └── Q9Reducer2.java
└── questions7And8
    ├── Q7_Q8Mapper1.java
    ├── Q7_Q8Mapper2.java
    ├── Q7_Q8Reducer1.java
    └── Q7_Q8Reducer2.java

pkg - contains MainClass.java, this class organizes the MapReduce jobs in order of execution.
Specifically for questions 7,8, and 9 there are chained MapReduce jobs with the second MapReduce
job depeding on the outputs from the first.
	StateMapper.java pulls the fields for questions 1-6 and outputs <Text (state), MapWritable>.  The MapWritable's key is a String
	of the form "Q#" and value is a delimeter seperated string containging the portions of data form this record needed to answer
	the particular analysis.  
	StateReducer.java iterates over the received values of type MapWritable and gets the data by the key "Q#".  It then splits on the 
	delimeter and sums the data for each field needed to complete the analysis.  It then builds the needed strings and outputs
	state name and answered questions.    

question9 - Q9Mapper1.java pulls the total population and age demographic data from the census input data and outputs
	    <state, demographic_data>.
	    Q9Reducer1.java sums the demographic data for each state from the mappers and outputs <state, demographic_data_sums> where
	    state is Text and demographic_data_sums is a Text consisting of 34 comma seperated values.  This reducer's output
	    serves as the input to Q9Mapper2.java.  
	    
	    Q9Mapper2.java simply passes all the demographic to a single reducer by setting key to NullWritable. 
	    Q9Reducer2.java calculates cosine similarity between states based on population age demographics.  

questions7And8 - The format to answer questions 7 and 8 is similar to that of 9.  Q7_Q8Mapper1.java pulls the needed data fields from the 
	    records outputs <state, data>.  Then first reducer Q7_Q8Reducer1.java sums the data and outputs to an intermediate file that is the 
	    input for the second MapReduce job.  Q7_Q8Mapper2.java consolidates all the data to a single reducer by setting key to NullWritable.
	    Q7_Q8Reducer2.java compares the data from all the states and writes the completed analysis.  
