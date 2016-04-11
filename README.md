# ElasticSearchPush

-
###Running the Application

To run the application you have to sbt intot he project

	sbt -java-home /opt/jdk1.8.0_71/

Then compile the application

	compile

To run the applicatin just use the run command.

	run

### Running the Application using noHup

Create an executable jar

	sbt package

then use the nohup command to run the application

	nohup scala jar/location &

To check logs or data written to console

	nohup <processExecuted> & tail -f nohup.out

