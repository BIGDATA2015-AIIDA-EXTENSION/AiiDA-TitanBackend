println "We start"
g = TitanFactory.open('conf/titan-hbase-es.properties')

file = new File(".", 'benchmark_resultQ2.txt')

startTime = new Date()

file << "start time $startTime.time\n"
file << "\n------------------------------------------------\n"
file << "starting Q2: Get all nodes with energy > 0.0f \n"
startQ2 = new Date()
q2 = g.V.has('energy', T.gt, 0.0f).count()
endQ2 = new Date()
file << "duration Q2: ${endQ2.time - startQ2.time}ms size: ${q2}\n"




endTime = new Date()
file << "\n------------------------------------------------\n"
file << "end time ${endTime.time}"
duration = endTime.time - startTime.time
file << "duration total ${duration}ms\n"
file << "duration in seconds ${duration/1000} s\n"
file << "duration in minutes ${duration/1000/60} m\n"


file << "===================================================\n"


g.shutdown()
