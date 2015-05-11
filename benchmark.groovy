println "We start"
g = TitanFactory.open('/Users/roger/EPFL/BigData/titan-0.5.4-hadoop2/conf/titan-hbase-es.properties')

startTime = new Date()
println "start time " +startTime.time

//firstQ = g.V.has('energy', T.lte, 0.0f).node_label.as('start').back(1).energy.as('energy').back(1).as('origin').in().has('jobresource_params.num_machines', T.gte, 4).as('inVertex')."jobresource_params.num_machines".as('numMachines').select
startSecond = new Date().time
secondQ = g.V.and(_().has("ELECTRONS.mixing_beta", T.gt, 0.0f), _().has("CONTROL.max_seconds", T.gt, 0))
endSecond = new Date().time
println "duration Second: " + (endSecond - startSecond) 

startSecond = new Date().time
thirdQ = g.V.and(_().has("element", "F"), _().has("energy", T.lte, 0.0f))
endSecond = new Date().time
println "duration third: " + (endSecond - startSecond) 

startSecond = new Date().time
fourthQ = g.V.has("element", "F").out.out.has('energy', T.lte, 0.0f)
endSecond = new Date().time
println "duration fourth: " + (endSecond - startSecond) 

startSecond = new Date().time
fifthQ = g.V.has("element", "F").out.out.has('energy', T.lte, 0.0f).as('parent').in.in.as('child2').in.as('child3').in.as('child4').in.as('child5').select
fifthQ = g.V.has("element", "F").out.out.has('energy', T.lte, 0.0f).as('parent').in.loop('parent'){it.loops <= 5}{it.loops >= 2}.path.as('children').select
endSecond = new Date().time
println "duration fifth: " + (endSecond - startSecond) 


endTime = new Date()

println "end time " + endTime.time
println "duration total" + (endTime.time - startTime.time) 


//first.each{println it}

g.shutdown()

