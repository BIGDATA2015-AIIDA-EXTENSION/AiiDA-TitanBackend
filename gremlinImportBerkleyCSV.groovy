// Adapt to the directory where your csv files lie
csvDir = "shared/aiida_100_export"
confDir = 'conf/titan-berkeleydb-es.properties'

maxInsertsBeforeCommit = 100000

println('cleaning old database')
//g = TitanFactory.open('/Users/roger/EPFL/BigData/titan-0.5.4-hadoop2/conf/titan-berkeleydb-es.properties')
//g = TitanFactory.open(confDir)
//
//g.shutdown()
//TitanCleanup.clear(g)

// Creating schema with batch-loading = false

//conf = new BaseConfiguration()
//conf.setProperty('storage.backend', 'berkeleyje')
//conf.setProperty('storage.directory', '/db/berkeley')
//conf.setProperty('index.search.backend', 'elasticsearch')
//conf.setProperty('index.search.directory', '/db/es')
//conf.setProperty('index.search.elasticsearch.client-only', false)
//conf.setProperty('index.search.elasticsearch.local-mode', true)



g = TitanFactory.build().
        set("storage.backend", "berkeleyje").
        set("storage.directory", "db/graph").
        set('index.search.backend', 'elasticsearch').
        set('index.search.directory', 'db/es').
        set('index.search.elasticsearch.client-only', false).
        set('index.search.elasticsearch.local-mode', true).open()

println ""
println "BUILDING SCHEMA"

println "defining vertex properties..."

// Create node properties
mgmt = g.getManagementSystem()
node_uuid = mgmt.makePropertyKey('node_uuid').dataType(String.class).make()
node_type = mgmt.makePropertyKey('node_type').dataType(String.class).make()
node_label = mgmt.makePropertyKey('node_label').dataType(String.class).make()
node_description = mgmt.makePropertyKey('node_description').dataType(String.class).make()
node_ctime = mgmt.makePropertyKey('node_ctime').dataType(Date.class).make()
node_mtime = mgmt.makePropertyKey('node_mtime').dataType(Date.class).make()
node_version = mgmt.makePropertyKey('node_version').dataType(Integer.class).make()
node_public = mgmt.makePropertyKey('node_public').dataType(Boolean.class).make()

// Create calcstate properties
calc_state = mgmt.makePropertyKey('calc_state').dataType(Integer.class).make()
calc_time = mgmt.makePropertyKey('calc_time').dataType(Date.class).make()

// Create comment properties
comment_uuid = mgmt.makePropertyKey('comment_uuid').dataType(String.class).make()
comment_ctime = mgmt.makePropertyKey('comment_ctime').dataType(Date.class).make()
comment_mtime = mgmt.makePropertyKey('comment_mtime').dataType(Date.class).make()
comment_content = mgmt.makePropertyKey('comment_content').dataType(String.class).make()

// Create computer properties
computer_uuid = mgmt.makePropertyKey('computer_uuid').dataType(String.class).make()
computer_name = mgmt.makePropertyKey('computer_name').dataType(String.class).make()
computer_hostname = mgmt.makePropertyKey('computer_hostname').dataType(String.class).make()
computer_description = mgmt.makePropertyKey('computer_description').dataType(String.class).make()
computer_transport_type = mgmt.makePropertyKey('computer_transport_type').dataType(String.class).make()
computer_scheduler_type = mgmt.makePropertyKey('computer_scheduler_type').dataType(String.class).make()
computer_metadata = mgmt.makePropertyKey('computer_metadata').dataType(String.class).make()

// Create group properties
group_uuid = mgmt.makePropertyKey('group_uuid').dataType(String.class).make()
group_name = mgmt.makePropertyKey('group_name').dataType(String.class).make()
group_type = mgmt.makePropertyKey('group_type').dataType(String.class).make()
group_time = mgmt.makePropertyKey('group_time').dataType(Date.class).make()
group_description = mgmt.makePropertyKey('group_description').dataType(String.class).make()

// Create user properties
user_password = mgmt.makePropertyKey('user_password').dataType(String.class).make()
user_last_login = mgmt.makePropertyKey('user_last_login').dataType(Date.class).make()
user_is_superuser = mgmt.makePropertyKey('user_is_superuser').dataType(Boolean.class).make()
user_email = mgmt.makePropertyKey('user_email').dataType(String.class).make()
user_first_name = mgmt.makePropertyKey('user_first_name').dataType(String.class).make()
user_last_name = mgmt.makePropertyKey('user_last_name').dataType(String.class).make()
user_institution = mgmt.makePropertyKey('user_institution').dataType(String.class).make()
user_is_staff = mgmt.makePropertyKey('user_is_staff').dataType(Boolean.class).make()
user_is_active = mgmt.makePropertyKey('user_is_active').dataType(Boolean.class).make()
user_date_joined = mgmt.makePropertyKey('user_date_joined').dataType(Date.class).make()

// Parse each property entry stored in properties.csv and creates it with the corresponding type
new File(csvDir + "/properties.csv").each({ line ->
    (key, type) = line.split(";")
    if (type == "float")
        mgmt.makePropertyKey(key).dataType(Float.class).make()
    else if (type == "int")
        mgmt.makePropertyKey(key).dataType(Integer.class).make()
    else if (type == "bool")
        mgmt.makePropertyKey(key).dataType(Boolean.class).make()
    else if (type == "date")
        mgmt.makePropertyKey(key).dataType(Date.class).make()
    else if (type == "txt")
        mgmt.makePropertyKey(key).dataType(String.class).make()

})

println "defining edge labels..."
labels = ['A', 'B', 'C', 'D', 'E', 'F']
mgmt.makeEdgeLabel('creates').make()
mgmt.makeEdgeLabel('computes').make()
mgmt.makeEdgeLabel('withCalcState').make()
mgmt.makeEdgeLabel('withComment').make()
mgmt.makeEdgeLabel('inGroup').make()
/*
// Create each edge label from labels.csv
new File(csvDir + "/labels.csv").each({ line ->
    mgmt.makeEdgeLabel(line).make()
})*/
for (int i = 0; i < labels.size(); i++) {
    mgmt.makeEdgeLabel(labels[i]).make()
}


println('creating indices...')

energy = mgmt.getPropertyKey('energy')
mgmt.buildIndex('mixedEnergy',Vertex.class).addKey(energy).buildMixedIndex("search")


numberOfAtmos = mgmt.getPropertyKey('number_of_atoms')
mgmt.buildIndex('mixedNumber_of_atoms',Vertex.class).addKey(numberOfAtmos).buildMixedIndex("search")


println "Committing the schema..."

mgmt.commit()

println ""
println "IMPORTING DATA"


println 'loading graph from hbase...'

g = TitanFactory.build().
        set("storage.backend", "berkeleyje").
        set("storage.directory", "db/graph").
        set('index.search.backend', 'elasticsearch').
        set('index.search.directory', 'db/es').
        set('index.search.elasticsearch.client-only', false).
        set('index.search.elasticsearch.local-mode', true).open()




bg = new BatchGraph(g, VertexIDType.STRING, 1000)

/*-------------------------------------- TITANS VERTICES CREATION FROM CSV FILES -------------------------------------*/
println 'importing nodes...'

counter = 0

// Parse db_dbnode entries stored in nodes.csv, creates a node vertex identified by id and add it to TitanGraph
new File(csvDir + "/nodes.csv").each({ line ->
    (id, uuid, type, node_label, description, ctime, mtime, user_id, computer_id, node_version, is_public) = line.split(";")

    attributes = [:]

    node = bg.addVertex("node::" + id)

    if (uuid && uuid != "null")
        attributes.put("node_uuid", uuid.toString())
    if (type && type != "null")
        attributes.put("node_type", type.toString())
    if (node_label && node_label != "null")
        attributes.put("node_label", node_label.toString())
    if (description && description != "null")
        attributes.put("node_description", description.toString())
    if (ctime && ctime != "null")
        attributes.put("node_ctime", Date.parse("yyyy-MM-dd H:m:s", ctime.toString()))
    if (mtime && mtime != "null")
        attributes.put("node_mtime", Date.parse("yyyy-MM-dd H:m:s", mtime.toString()))
    if (node_version && node_version != "null")
        attributes.put("node_version", node_version.toInteger())
    if (is_public && is_public !="null")
        attributes.put("node_public", is_public.toBoolean())

    attributes.put("node_type", "node")
    ElementHelper.setProperties(node, attributes)

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }

})

bg.commit()

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

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }


})

bg.commit()


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

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }
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
        attributes.put("calc_state", id.toInteger())
    if (time && time != "null")
        attributes.put("calc_time", Date.parse("yyyy-MM-dd H:m:s", time.toString()))

    attributes.put("node_type", "calcstate")
    ElementHelper.setProperties(node, attributes)

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }

})

println 'importing comments...'

// Parse db_dbcomment entries stored in comments.csv, creates a comment vertex identified by id and add it to TitanGraph
new File(csvDir + "/comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(";")

    attributes = [:]

    node = bg.addVertex("comment::" + id)

    if (uuid && uuid != "null")
        attributes.put("comment_uuid", uuid.toString())
    if(ctime && ctime == "null")
        attributes.put("comment_ctime", Date.parse("yyyy-MM-dd H:m:s", ctime.toString()))
    if (mtime && mtime != "null")
        attributes.put("comment_mtime", Date.parse("yyyy-MM-dd H:m:s", mtime.toString()))
    if(content && content == "null")
        attributes.put("comment_content", content.toString())

    attributes.put("node_type", "comment")
    ElementHelper.setProperties(node, attributes)

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }

})

println 'importing computers...'

new File(csvDir + "/computers.csv").each({ line ->
    (id, uuid, name, hostname, description, enabled, transport_type, scheduler_type, transport_params, metadata) = line.split(";")

    attributes = [:]

    node = bg.addVertex("computer::" + id)

    if (uuid && uuid == "null")
        attributes.put("computer_uuid", uuid.toString())
    if (name && name != "null")
        attributes.put("computer_name", name .toString())
    if (hostname && hostname == "null")
        attrubutes.put("computer_hostname", hostname.toString())
    if (description && description != "null")
        attributes.put("computer_description", description.toString())
    if (transport_type && transport_type != "null")
        attributes.put("computer_transport_type", transport_type.toString())
    if (scheduler_type && scheduler_type != "null")
        attributes.put("computer_scheduler_type", scheduler_type.toString())
    if (transport_params && transport_params != "null")
        attributes.put("computer_metadata", metadata.toString())

    attributes.put("node_type", "computer")
    ElementHelper.setProperties(node, attributes)

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }

})

println 'importing groups...'

// Parses db_dbgroup entries stored in groups.csv, creates a group vertex identified by id
// and add it to TitanGraph
new File(csvDir + "/groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(";")

    attributes = [:]

    node = bg.addVertex("group::" + id)

    if (uuid && uuid != "null")
        attributes.put("group_uuid", uuid.toString())
    if (name && name != "null")
        attributes.put("group_name", name.toString())
    if (type && type != "null")
        attributes.put("group_type", type.toString())
    if (time && time != "null")
        attributes.put("group_time", Date.parse("yyyy-MM-dd H:m:s", time.toString()))
    if (description && description != "null")
        attributes.put("group_description", description.toString())

    attributes.put("node_type", "group")
    ElementHelper.setProperties(node, attributes)

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }
})

println 'importing users...'

// Parse db_dbuser entries stored in users.csv, creates a user vertex identified by id and add it to TitanGraph
new File(csvDir + "/users.csv").each({ line ->
    (id, password, last_login, is_superuser, email, first_name, last_name, institution, is_staff, is_active, date_joined) = line.split(";")

    attributes = [:]

    node = bg.addVertex("user::" + id)

    if (password && password != "null")
        attributes.put("user_password", password.toString())
    if (last_login && last_login != "null")
        attributes.put("user_last_login", Date.parse("yyyy-MM-dd H:m:s", last_login.toString()))
    if (is_superuser && is_superuser != "null")
        attributes.put("user_is_superuser", is_superuser.toBoolean())
    if (email && email != "null")
        attributes.put("user_email", email.toString())
    if (first_name && first_name != "null")
        attributes.put("user_first_name", first_name.toString())
    if (last_name && last_name != "null")
        attributes.put("user_last_name", last_name.toString())
    if (institution && institution != "null")
        attributes.put("user_institution", institution.toString())
    if (is_staff && is_staff != "null")
        attributes.put("user_is_staff", is_staff.toBoolean())
    if (is_active && is_active != "null")
        attributes.put("user_is_active", is_active.toBoolean())
    if (date_joined && date_joined != "null")
        attributes.put("user_date_joined", Date.parse("yyyy-MM-dd H:m:s", date_joined.toString()))

    attributes.put("node_type", "user")
    ElementHelper.setProperties(node, attributes)

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }
})

bg.commit()

/*--------------------------------------- TITANS EDGES CREATION FROM CSV FILES ---------------------------------------*/

println 'linking nodes to nodes...'

idx = 0
// Create edges between each node vertex given links.csv extracted from db_dblinks
// Edge labels represent the relationship between the two linked nodes
new File(csvDir + "/links.csv").each({ line ->
    (id, input_id, output_id, label) = line.split(";")

    source = bg.getVertex("node::" + input_id)
    target = bg.getVertex("node::" + output_id)

    bg.addEdge(null, source, target, labels[idx%labels.size()])
    counter++
    idx++
    if (counter > maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
    }
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

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }


})

println 'linking nodes to calcstates...'

// Create edges between node vertices and calcState vertices extracted from calcstates.csv
// Edge labels represent the fact that a node has a given calculation state
new File(csvDir + "/calcstates.csv").each({ line ->
    (id, node_id, state, time) = line.split(";")

    node = bg.getVertex("node::" + node_id)
    calcState = bg.getVertex("calcstate::" + id)

    bg.addEdge(null, node, calcState, 'withCalcState')

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }
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

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }

})

println 'linking users to groups'

new File(csvDir + "/groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(";")

    group = bg.getVertex("group::" + id)
    user = bg.getVertex("node::" + user_id)
    bg.addEdge(null, user, group, 'inGroup')

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }
})

println 'linking nodes to groups'

new File(csvDir + "/nodegroups.csv").each({ line ->
    (id, group_id, node_id) = line.split(";")

    group = bg.getVertex("group::" + group_id)
    node = bg.getVertex("node::" + node_id)
    bg.addEdge(null, node, group, 'inGroup')

    counter++
    if (counter >= maxInsertsBeforeCommit) {
        counter = 0
        bg.commit()
        println "committed"
    }
})

bg.commit()
g.commit()

println ""
println 'GRAPH SUCCESSFULLY CREATED'

bg.shutdown()
g.shutdown()

