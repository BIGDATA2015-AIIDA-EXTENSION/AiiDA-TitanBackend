// Adapt to the directory where your csv files lie
csvDir = "aiida_export"
confDir = 'conf/titan-hbase-es.properties'

println('cleaning old database')
//g = TitanFactory.open('/Users/roger/EPFL/BigData/titan-0.5.4-hadoop2/conf/titan-berkeleydb-es.properties')
g = TitanFactory.open(confDir)

g.shutdown()
TitanCleanup.clear(g)

println 'loading graph from hbase...'

g = TitanFactory.open(confDir)


println('creating indices')

mgmt = g.getManagementSystem()
element = mgmt.makePropertyKey('element').dataType(String.class).make()
mgmt.buildIndex('byElement',Vertex.class).addKey(element).buildCompositeIndex()
mgmt.commit()

mgmt = g.getManagementSystem()
energy = mgmt.makePropertyKey('energy').dataType(Float.class).make()
mgmt.buildIndex('mixedEnergy',Vertex.class).addKey(energy).buildMixedIndex("search")
mgmt.commit()

mgmt = g.getManagementSystem()
energy = mgmt.makePropertyKey('ELECTRONS.mixing_beta').dataType(Float.class).make()
mgmt.buildIndex('mixedELECTRONS.mixing_beta',Vertex.class).addKey(energy).buildMixedIndex("search")
mgmt.commit()

mgmt = g.getManagementSystem()
energy = mgmt.makePropertyKey('CONTROL.max_seconds').dataType(Float.class).make()
mgmt.buildIndex('mixedCONTROL.max_seconds',Vertex.class).addKey(energy).buildMixedIndex("search")
mgmt.commit()

mgmt = g.getManagementSystem()
nodeType = mgmt.makePropertyKey('node_type').dataType(String.class).make()
mgmt.buildIndex('mixedNode_type',Vertex.class).addKey(nodeType).buildMixedIndex("search")
mgmt.commit()



mgmt = g.getManagementSystem()
numberOfAtmos = mgmt.makePropertyKey('number_of_atoms').dataType(Integer.class).make()
mgmt.buildIndex('mixedNumber_of_atoms',Vertex.class).addKey(numberOfAtmos).buildMixedIndex("search")
mgmt.commit()

println 'loading graph from hbase...'

bg = new BatchGraph(g, VertexIDType.STRING, 1000)

/*-------------------------------------- TITANS VERTICES CREATION FROM CSV FILES -------------------------------------*/
println 'importing nodes...'

// Parse db_dbnode entries stored in nodes.csv, creates a node vertex identified by id and add it to TitanGraph
new File(csvDir + "/nodes.csv").each({ line ->
    (id, uuid, type, node_label, description, ctime, mtime, user_id, computer_id, node_version, is_public) = line.split(";")

    attributes = [:]

    node = bg.addVertex("node::" + id)

    if (uuid && uuid != "null")
        attributes.put("uuid", uuid.toString())
    if (type && type != "null")
        attributes.put("type", type.toString())
    if (node_label && node_label != "null")
        attributes.put("node_label", node_label.toString())
    if (description && description != "null")
        attributes.put("description", description.toString())
    if (ctime && ctime != "null")
        attributes.put("ctime", Date.parse("yyyy-MM-dd H:m:s", ctime.toString()))
    if (mtime && mtime != "null")
        attributes.put("mtime", Date.parse("yyyy-MM-dd H:m:s", mtime.toString()))
    if (node_version && node_version != "null")
        attributes.put("node_version", node_version.toInteger())
    if (is_public && is_public !="null")
        attributes.put("public", is_public.toBoolean())

    attributes.put("node_type", "node")
    ElementHelper.setProperties(node, attributes)



})

println 'importing attributes...'
def tmp_attr = [:]
def tmp_id = -1
// Parse db_dbattribute entries stored in attribute.csv, creates an attribute vertex identified by id
// and add it to TitanGraph. Switch case ensure the correct type for the value
// TODO: Take into account datatype of type 'list' or 'dict' take into account tval
new File(csvDir + "/attributes.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(";")

    if (tmp_id.toInteger() == -1 ) {
        tmp_id = node_id.toInteger()
    }

    if (tmp_id.toInteger() != node_id.toInteger()) {
        node = bg.getVertex("node::" + tmp_id)
        attr = ElementHelper.getProperties(node)
        new_attr = attr + tmp_attr
        ElementHelper.setProperties(node, new_attr)
        tmp_id = node_id.toInteger()
        tmp_attr = [:]
    }

    if (datatype == "float")
        tmp_attr.put(key.toString(), fval.toFloat())
    else if (datatype == "int")
        tmp_attr.put(key.toString(), ival.toInteger())
    else if (datatype == "bool")
        tmp_attr.put(key.toString(), bval.toBoolean())
    else if (datatype == "date")
        tmp_attr.put(key.toString(), Date.parse("yyyy-MM-dd H:m:s", dval.toString()))
    else if (datatype == "txt")
        tmp_attr.put(key.toString(), tval.toString())


})

//When the last line has been read set new properties for the last node
if (tmp_id.toInteger() != -1) {
    node = bg.getVertex("node::" + tmp_id)
    attr = ElementHelper.getProperties(node)
    new_attr = attr + tmp_attr
    ElementHelper.setProperties(node, new_attr)
    tmp_id = -1
    tmp_attr = [:]
}


println 'importing extras...'

// Parse db_dbextra entries stored in extras.csv, creates an extra vertex identified by id and add it to TitanGraph.
// Switch case ensure the correct type for the value
// TODO: Take into account datatype of type 'list' or 'dict', take into account tval
new File(csvDir + "/extras.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(";")

    if (tmp_id.toInteger() == -1 ) {
        tmp_id = node_id.toInteger()
    }

    if (tmp_id.toInteger() != node_id.toInteger()) {
        node = bg.getVertex("node::" + tmp_id)
        attr = ElementHelper.getProperties(node)
        new_attr = attr + tmp_attr
        ElementHelper.setProperties(node, new_attr)
        tmp_id = node_id.toInteger()
        tmp_attr = [:]
    }

    if (datatype == "float")
        tmp_attr.put(key.toString(), fval.toFloat())
    else if (datatype == "int")
        tmp_attr.put(key.toString(), ival.toInteger())
    else if (datatype == "bool")
        tmp_attr.put(key.toString(), bval.toBoolean())
    else if (datatype == "date")
        tmp_attr.put(key.toString(), Date.parse("yyyy-MM-dd H:m:s", dval.toString()))
    else if (datatype == "txt")
        tmp_attr.put(key.toString(), tval.toString())
})

if (tmp_id.toInteger() != -1) {
    node = bg.getVertex("node::" + tmp_id)
    attr = ElementHelper.getProperties(node)
    new_attr = attr + tmp_attr
    ElementHelper.setProperties(node, new_attr)
    tmp_id = -1
    tmp_attr = [:]
}

println 'importing calcstates...'

// Parse db_dbcalcstate entries stored in calcstates.csv, creates a calcState vertex identified by id
// and add it to TitanGraph
new File(csvDir + "/calcstates.csv").each({ line ->
    (id, node_id, state, time) = line.split(";")

    attributes = [:]

    node = bg.addVertex("calcstate::" + id)

    if (state && state != "null")
        attributes.put("state", id.toInteger())
    if (time && time != "null")
        attributes.put("time", Date.parse("yyyy-MM-dd H:m:s", time.toString()))

    attributes.put("node_type", "calcstate")
    ElementHelper.setProperties(node, attributes)

})

println 'importing comments...'

// Parse db_dbcomment entries stored in comments.csv, creates a comment vertex identified by id and add it to TitanGraph
new File(csvDir + "/comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(";")

    attributes = [:]

    node = bg.addVertex("comment::" + id)

    if (uuid && uuid != "null")
        attributes.put("uuid", uuid.toString())
    if(ctime && ctime == "null")
        attributes.put("ctime", Date.parse("yyyy-MM-dd H:m:s", ctime.toString()))
    if (mtime && mtime != "null")
        attributes.put("mtime", Date.parse("yyyy-MM-dd H:m:s", mtime.toString()))
    if(content && content == "null")
        attributes.put("content", content.toString())

    attributes.put("node_type", "comment")
    ElementHelper.setProperties(node, attributes)

})

println 'importing computers...'

new File(csvDir + "/computers.csv").each({ line ->
    (id, uuid, name, hostname, description, enabled, transport_type, scheduler_type, transport_params, metadata) = line.split(";")

    attributes = [:]

    node = bg.addVertex("computer::" + id)

    if (uuid && uuid == "null")
        attributes.put("uuid", uuid.toString())
    if (name && name != "null")
        attributes.put("name", name .toString())
    if (hostname && hostname == "null")
        attrubutes.put("hostname", hostname.toString())
    if (description && description != "null")
        attributes.put("description", description.toString())
    if (transport_type && transport_type != "null")
        attributes.put("transport_type", transport_type.toString())
    if (scheduler_type && scheduler_type != "null")
        attributes.put("scheduler_type", scheduler_type.toString())
    if (transport_params && transport_params != "null")
        attributes.put("metadata", metadata.toString())

    attributes.put("node_type", "computer")
    ElementHelper.setProperties(node, attributes)

})

println 'importing groups...'

// Parses db_dbgroup entries stored in groups.csv, creates a group vertex identified by id
// and add it to TitanGraph
new File(csvDir + "/groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(";")

    attributes = [:]

    node = bg.addVertex("group::" + id)

    if (uuid && uuid != "null")
        attributes.put("uuid", uuid.toString())
    if (name && name != "null")
        attributes.put("name", name.toString())
    if (type && type != "null")
        attributes.put("type", type.toString())
    if (time && time != "null")
        attributes.put("time", Date.parse("yyyy-MM-dd H:m:s", time.toString()))
    if (description && description != "null")
        attributes.put("description", description.toString())

    attributes.put("node_type", "group")
    ElementHelper.setProperties(node, attributes)
})

println 'importing users...'

// Parse db_dbuser entries stored in users.csv, creates a user vertex identified by id and add it to TitanGraph
new File(csvDir + "/users.csv").each({ line ->
    (id, password, last_login, is_superuser, email, first_name, last_name, institution, is_staff, is_active, date_joined) = line.split(";")

    attributes = [:]

    node = bg.addVertex("user::" + id)

    if (password && password != "null")
        attributes.put("password", password.toString())
    if (last_login && last_login != "null")
        attributes.put("last_login", Date.parse("yyyy-MM-dd H:m:s", last_login.toString()))
    if (is_superuser && is_superuser != "null")
        attributes.put("is_superuser", is_superuser.toBoolean())
    if (email && email != "null")
        attributes.put("email", email.toString())
    if (first_name && first_name != "null")
        attributes.put("first_name", first_name.toString())
    if (last_name && last_name != "null")
        attributes.put("last_name", last_name.toString())
    if (institution && institution != "null")
        attributes.put("institution", institution.toString())
    if (is_staff && is_staff != "null")
        attributes.put("is_staff", is_staff.toBoolean())
    if (is_active && is_active != "null")
        attributes.put("is_active", is_active.toBoolean())
    if (date_joined && date_joined != "null")
        attributes.put("date_joined", Date.parse("yyyy-MM-dd H:m:s", date_joined.toString()))

    attributes.put("node_type", "user")
    ElementHelper.setProperties(node, attributes)
})


/*--------------------------------------- TITANS EDGES CREATION FROM CSV FILES ---------------------------------------*/

println 'linking nodes to nodes...'

// Create edges between each node vertex given links.csv extracted from db_dblinks
// Edge labels represent the relationship between the two linked nodes
new File(csvDir + "/links.csv").each({ line ->
    (id, input_id, output_id, label) = line.split(";")

    source = bg.getVertex("node::" + input_id)
    target = bg.getVertex("node::" + output_id)

    bg.addEdge(null, source, target, label)
})

println 'linking users and computers to nodes...'
// Create edges between computer vertices and node vertices given in ComputerToNode.csv (in this file there is no edges
// pointing to null). Edge labels represent the fact that a computer compute a code/data/calculation (a.k.a node)
new File(csvDir + "/nodes.csv").each({ line ->
    (node_id, uuid, type, label, description, ctime, mtime, user_id, computer_id, node_version, is_public) = line.split(";")

    node = bg.getVertex("node::" + node_id)

    // Link computers to nodes
    if (computer_id && computer_id != "null") {
        computer = bg.getVertex("computer::" + computer_id)
        bg.addEdge(null, computer, node, 'computes')
    }

    // Link users to nodes
    user = bg.getVertex("user::" + user_id)
    bg.addEdge(null, user, node, 'creates')


})

println 'linking nodes to calcstates...'

// Create edges between node vertices and calcState vertices extracted from calcstates.csv
// Edge labels represent the fact that a node has a given calculation state
new File(csvDir + "/calcstates.csv").each({ line ->
    (id, node_id, state, time) = line.split(";")

    node = bg.getVertex("node::" + node_id)
    calcState = bg.getVertex("calcstate::" + id)

    bg.addEdge(null, node, calcState, 'withCalcState')
})

println 'linking nodes and users to comments...'

// Create edges between node vertices and comment vertices extracted from comments.csv
// Edge labels represent the fact that a node has a given comment
new File(csvDir + "/comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(";")

    // Link nodes to comments
    node = bg.getVertex("node::" + node_id)
    comment = bg.getVertex("comment::" + id)
    bg.addEdge(null, node, comment, 'withComment')

})

println 'linking users to groups'

new File(csvDir + "/groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(";")

    group = bg.getVertex("group::" + id)
    user = bg.getVertex("node::" + user_id)
    bg.addEdge(null, user, group, 'inGroup')
})

println 'linking nodes to groups'

new File(csvDir + "/nodegroups.csv").each({ line ->
    (id, group_id, node_id) = line.split(";")

    group = bg.getVertex("group::" + group_id)
    node = bg.getVertex("node::" + node_id)
    bg.addEdge(null, node, group, 'inGroup')
})

bg.commit()

println 'GRAPH SUCCESSFULLY CREATED'

g.shutdown()
