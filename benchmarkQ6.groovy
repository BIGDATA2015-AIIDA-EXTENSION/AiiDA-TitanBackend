println "We start"
g = TitanFactory.open('conf/titan-hbase-es.properties')

file = new File(".", 'benchmark_resultQ6.txt')

startTime = new Date()

file << "start time $startTime.time\n"
file << "\n------------------------------------------------\n"
file << "starting Q4: Get the input  node 3 levels up and all its attributes of the nodes with\n" +
        "- number_of_atoms > 3 \n" +
        "- energy > 0\n"
startQ4 = new Date()
q4 = g.V.has('number_of_atoms', T.gt, 3).has('energy', T.gt, 0.0f).in('A').in('A').in('A').map.dedup().count()
endQ4 = new Date()
file << "duration Q6: ${endQ4.time - startQ4.time}ms size: ${q4}\n"
file << "duration Q6: ${(endQ4.time - startQ4.time)/1000/60} m size: ${q4}\n"


endTime = new Date()
file << "\n------------------------------------------------\n"
file << "end time ${endTime.time}"
duration = endTime.time - startTime.time
file << "duration total ${duration}ms\n"
file << "duration in seconds ${duration/1000} s\n"
file << "duration in minutes ${duration/1000/60} m\n"


file << "===================================================\n"


g.shutdown()
