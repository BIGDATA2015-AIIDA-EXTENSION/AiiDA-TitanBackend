println "We start"
g = TitanFactory.open('../conf/titan-berkeleydb-es.properties')

file = new File(".", 'benchmark_resultQ3.txt')

startTime = new Date()

file << "start time $startTime.time\n"
file << "\n------------------------------------------------\n"
file << "starting Q3: Get the input node of the nodes with\n" +
        "- number_of_atoms > 3 \n" +
        "- energy > 0\n"
startQ3 = new Date()
q3 = g.V.has('number_of_atoms', T.gt, 3).has('energy', T.gt, 0.0f).in('A').dedup().count()
endQ3 = new Date()
file << "duration Q3: ${endQ3.time - startQ3.time}ms size: ${q3}\n"
file << "duration Q3: ${(endQ3.time - startQ3.time)/1000/60} m size: ${q3}\n"



endTime = new Date()
file << "\n------------------------------------------------\n"
file << "end time ${endTime.time}"
duration = endTime.time - startTime.time
file << "duration total ${duration}ms\n"
file << "duration in seconds ${duration/1000} s\n"
file << "duration in minutes ${duration/1000/60} m\n"

println("duration: " + duration)
file << "===================================================\n"


g.shutdown()
