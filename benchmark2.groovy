println "We start"
g = TitanFactory.open('conf/titan-hbase-es.properties')

file = new File(".", 'benchmark_result.txt')

startTime = new Date()

file << "start time $startTime.time\n"
file << "\n------------------------------------------------\n"
file << "starting Q1: Get all nodes with energy <= 0.0f \n"
startQ1 = new Date()
q1 = g.V.has('energy', T.lte, 0.0f).count()
endQ1 = new Date()
file << "duration Q1: ${endQ1.time - startQ1.time}ms size: ${q1}"


file << "\n------------------------------------------------\n"
file << "starting Q2: Get all nodes with energy > 0.0f \n"
startQ2 = new Date()
q2 = g.V.has('energy', T.gt, 0.0f).count()
endQ2 = new Date()
file << "duration Q2: ${endQ2.time - startQ2.time}ms size: ${q2}\n"

file << "\n------------------------------------------------\n"
file << "starting Q3: Get the input node of the nodes with\n" +
        "- input number_of_atoms > 3 \n" +
        "- output energy > 0\n"
startQ3 = new Date()
q3 = g.V.has('number_of_atoms', T.gt, 3).has('energy', T.gt, 0.0f).in.dedup().count()
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
