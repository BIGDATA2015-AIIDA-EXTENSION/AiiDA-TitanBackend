println "We start"
g = TitanFactory.open('conf/titan-hbase-es.properties')

file = new File(".", 'benchmark_result.txt')

startTime = new Date()

file << "start time $startTime.time\n"
file << "starting Q1: Get all nodes with energy <= 0.0f \n"
startQ1 = new Date()
q1 = g.V.has('node_type', 'node').out.has('energy', T.lte, 0.0f).count()
endQ1 = new Date()
file << "duration Q1: ${endQ1.time - startQ1.time} size: ${q1}"

file << "starting Q2: Get all nodes with energy > 0.0f \n"
startQ2 = new Date()
q2 = g.V.has('node_type', 'node').out.has('energy', T.gt, 0.0f).count()
endQ2 = new Date()
file << "duration Q2: ${endQ2.time - startQ2.time} size: ${q2}"


file << "starting Q3: Get all nodes with\n" +
        "- input number_of_atoms > 3 \n" +
        "- output energy <= 0\n" +
        "from these get the the input nodes two to 5 levels up"
startQ3 = new Date()
q3 = g.V.has('number_of_atoms', T.gt, 3).has('energy', T.gt, 0.0f).as('origin').in.in.as('c2').in.as('c3').in.as('c4').in.as('c5').select.count()
endQ3 = new Date()
file << "duration Q3: ${endQ3.time - startQ3.time} size: ${q3}"

endTime = new Date()

println "end time " + endTime.time
println "duration total " + (endTime.time - startTime.time)


g.shutdown()

/*

akear [11:31 PM]11:31
sub_qt = QueryTool(Calculation)
sub_qt.filter_input_attr("number_of_atoms", ">", 3")
sub_qt.filter_output_attr("energy", "<=", 0.)

qt = QueryTool()
qt.filter_relation("parents", sub_qt, min_depth=2, max_depth=5)
 */

/*
file << "starting Q3: Get all nodes with type Calculation\n" +
        "with input number_of_atoms > 3 \n" +
        "with output energy <= 0"
startQ2 = new Date()
q2 = g.V.has('number_of_atoms', T.gt, 3).as('in').out.as('origin').out.has('energy', T.gt, 0.0f).as('out').select.count()
endQ2 = new Date()
file << "duration Q2: ${endQ2.time - startQ2.time} size: ${q2}"
*/