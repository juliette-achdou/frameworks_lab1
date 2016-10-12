# TP 2 Hadoop
## Launching the project

I got a jar file, called TP-1.0-SNAPSHOT.jar out of my maven project,
using the command package for maven project on IntelliJ IDEA.

Then I sent it on the server using scp

And then I used the command :
hadoop jar myjar TP1/"The class I want for the main" input output 

where input is :  /res/prenoms.csv

## Results
You can find the result directories in my home directory on the server , in the directory outputs,
under the names: output1, output2, output3

The result in output3.csv may seem strange because it does not adds up to 1 :
as commented,
it is because I chose to give that the probability that a name is for example at least feminine.

The result in output1.csv contains an empty word, which corresponds to all empty origins field.
I could have removed it, but found it interesting.
