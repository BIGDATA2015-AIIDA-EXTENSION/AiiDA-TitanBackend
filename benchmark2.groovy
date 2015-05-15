println "We start"
g = TitanFactory.open('conf/titan-hbase-es.properties')
startTime = new Date()
println "start time " +startTime.time

startQ1 = new Date()
q1 = g.V.has('node_type', 'node').out.has('energy', T.lte, 0.0f).count()
endQ1 = new Date()
println "duration Q1: " + (endQ1.time - startQ1.time) + " size: " + q1

endTime = new Date()

println "end time " + endTime.time
println "duration total " + (endTime.time - startTime.time)


g.shutdown()

