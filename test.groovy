
println "We start"


g = TitanFactory.open('/Users/roger/EPFL/BigData/titan-0.5.4-hadoop2/conf/titan-hbase-es.properties')

println "we are here"
t = new Table()
g.V.has("age", T.lte, 1000).as('young').out('battled').has("name", "cerberus").name.as('planet').back('young').name.as('father').table(t)

g.V.sideEffect{println it.map.next()}.iterate()
println g.V.map
println t


g.shutdown()
//t = new Table()
//g.V.has("age", T.lte, 1000).as('young').out('battled').has("name", "cerberus").name.as('planet').back('young').name.as('father').table(t)
//TitanCleanup.clear(g)

//g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O'}.dedup.node_label.as('element').table(t)

g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').copySplit(
	_().in().loop('origin'){it.loops < 10}{it.object.element == 'O'}.dedup.node_label.as('element'), 
	_().in().loop('origin'){it.loops < 10}{it.object.energy < 0}.dedup.node_label.as('energy').back(1).energy.as('energy_value')
).fairMerge.table(t)


g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O' || it.object.energy < 0.0f }.dedup.copySplit(
	_().has('element').node_label.as('element'), 
	_().has('energy').node_label.as('energy')
).exhaustMerge.select()

g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O' || (it.object.energy != null && it.object.energy < 0.0f) }.dedup.node_label.as("stuff").select


g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O' || (it.object.energy != null && it.object.energy < 0.0f) }.dedup.copySplit(
	_().has('element').node_label.as('element_label').back(1).element.as('element'), 
	_().has('energy').node_label.as('energy_label').back(1).energy.as('energy'),
	_().path.as('path')
).enablePath().fairMerge.select().collect()


g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O'}.dedup.node_label.as("stuff").select

g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O'}.dedup.path.filter{it.contains(g.v(16448L))}


g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O' || (it.object.energy != null && it.object.energy < 0.0f) }.dedup.ifThenElse{it.element != null}{it.element}.as('element')({it.energy}.as("stuff").select


	x.findAll{!it.contains(null)}.collect{it[0..5]}



g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O' || (it.object.energy != null && it.object.energy < 0.0f) }.dedup.copySplit(
	_().has('element').node_label.as('element_label').back(1).element.as('element'), 
	_().has('energy').node_label.as('energy_label').back(1).energy.as('energy'),
	_().path{it.node_label}.as('path')
).enablePath().fairMerge.select().collect{it[0..6]}.findAll{!it.contains(null)}

g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').in().loop('origin'){it.loops < 10}{it.object.element == 'O'}.node_label.as('element').back(1).out().loop(1){it.loops < 10}{it.object.energy < 0.0f}.node_label.as('energy').dedup.select


g.V.has('energy', T.gte, 0.0f).node_label.as('start').back(1).as('origin').or(_().inE("parameters"), _().inE("output_parameters")).loop('origin'){it.loops < 10}{it.object.element == 'O'}.node_label.as('element').back(1).out().loop(1){it.loops < 10}{it.object.energy < 0.0f}.node_label.as('energy').dedup.select
