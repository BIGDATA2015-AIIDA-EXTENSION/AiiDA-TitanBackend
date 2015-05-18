println "We start"
g = TitanFactory.open('../conf/titan-berkeleydb-es.properties')

file = new File(".", 'benchmark_resultQ1.txt')

startTime = new Date()

file << "start time $startTime.time\n"
file << "\n------------------------------------------------\n"
file << "starting Q1: Get all nodes with energy <= 0.0f \n"
startQ1 = new Date()
q1 = g.V.has('energy', T.lte, 0.0f).count()
endQ1 = new Date()
file << "duration Q1: ${endQ1.time - startQ1.time}ms size: ${q1}"



endTime = new Date()
file << "\n------------------------------------------------\n"
file << "end time ${endTime.time}"
duration = endTime.time - startTime.time
file << "duration total ${duration}ms\n"
file << "duration in seconds ${duration/1000} s\n"
file << "duration in minutes ${duration/1000/60} m\n"


file << "===================================================\n"


g.shutdown()

