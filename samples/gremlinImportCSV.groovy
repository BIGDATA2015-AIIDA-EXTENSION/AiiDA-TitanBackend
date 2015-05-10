// Adapt to the directory where your csv files lie
csvDir = "/Users/drissielkamili/Desktop/Cours/Big_Data/Project/aiida_titan_git/Titan/samples/"

println 'loading graph from hbase...'

g = TitanFactory.open("conf/titan-hbase.properties")
bg = new BatchGraph(g, VertexIDType.STRING, 1000)

/*-------------------------------------- TITANS VERTICES CREATION FROM CSV FILES -------------------------------------*/

println 'importing nodes...'

// Parse db_dbnode entries stored in nodes.csv, creates a node vertex identified by id and add it to TitanGraph
new File(csvDir + "nodes.csv").each({ line ->
    (id, uuid, type, node_label, description, ctime, mtime, user_id, computer_id, node_version, is_public) = line.split(";")

    attributes = [:]

    if (id)
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
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)

})

println 'importing attributes...'

// Parse db_dbattribute entries stored in attribute.csv, creates an attribute vertex identified by id
// and add it to TitanGraph. Switch case ensure the correct type for the value
// TODO: Take into account datatype of type 'list' or 'dict' take into account tval
new File(csvDir + "attributes.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(";")

    attributes = [:]

    if (id)
        node = bg.addVertex("attribute::" + id)
        attributes.put("key", key.toString())

        if (datatype == "float")
            attributes.put("value", fval.toFloat())
        else if (datatype == "int")
            attributes.put("value", ival.toInteger())
        else if (datatype == "bool")
            attributes.put("value", bval.toBoolean())
        else if (datatype == "date")
            attributes.put("value", Date.parse("yyyy-MM-dd H:m:s", dval.toString()))
        else if (datatype == "txt")
            attributes.put("value", tval.toString())
        else if (datatype == "list")
            attributes.put("value", "list")
        else if (datatype == "dict")
            attributes.put("value", "dict")

        attributes.put("node_type", "attribute")
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)

})

println 'importing extras...'

// Parse db_dbextra entries stored in extras.csv, creates an extra vertex identified by id and add it to TitanGraph.
// Switch case ensure the correct type for the value
// TODO: Take into account datatype of type 'list' or 'dict', take into account tval
new File(csvDir + "extras.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(";")

    attributes = [:]

    if (id)
        node = bg.addVertex("extra::" + id)
        attributes.put("key", key.toString())

        if (datatype == "float")
            attributes.put("value", fval.toFloat())
        else if (datatype == "int")
            attributes.put("value", ival.toInteger())
        else if (datatype == "bool")
            attributes.put("value", bval.toBoolean())
        else if (datatype == "date")
            attributes.put("value", Date.parse("yyyy-MM-dd H:m:s", dval.toString()))
        else if (datatype == "txt")
            attributes.put("value", tval.toString())
        else if (datatype == "list")
            attributes.put("value", "list")
        else if (datatype == "dict")
            attributes.put("value", "dict")

        attributes.put("node_type", "extra")
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)
})

println 'importing calcstates...'

// Parse db_dbcalcstate entries stored in calcstates.csv, creates a calcState vertex identified by id
// and add it to TitanGraph
new File(csvDir + "calcstates.csv").each({ line ->
    (id, node_id, state, time) = line.split(";")

    attributes = [:]

    if (id)
        node = bg.addVertex("calcstate::" + id)

        if (state && state != "null")
            attributes.put("state", id.toInteger())
        if (time && time != "null")
            attributes.put("time", Date.parse("yyyy-MM-dd H:m:s", time.toString()))

        attributes.put("node_type", "calcstate")
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)

})

println 'importing comments...'

// Parse db_dbcomment entries stored in comments.csv, creates a comment vertex identified by id and add it to TitanGraph
new File(csvDir + "comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(";")

    attributes = [:]

    if(id)
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
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)

})

println 'importing computers...'

new File(csvDir + "computers.csv").each({ line ->
    (id, uuid, name, hostname, description, enabled, transport_type, scheduler_type, transport_params, metadata) = line.split(";")

    attributes = [:]

    if(id)
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
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)
})

println 'importing groups...'

// Parses db_dbgroup entries stored in groups.csv, creates a group vertex identified by id
// and add it to TitanGraph
new File(csvDir + "groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(";")

    attributes = [:]

    if (id)
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
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)
})

println 'importing users...'

// Parse db_dbuser entries stored in users.csv, creates a user vertex identified by id and add it to TitanGraph
new File(csvDir + "users.csv").each({ line ->
    (id, password, last_login, is_superuser, email, first_name, last_name, institution, is_staff, is_active, date_joined) = line.split(";")

    attributes = [:]

    if (id)
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
        attributes.put("node_id", id.toInteger())
        ElementHelper.setProperties(node, attributes)
})


/*--------------------------------------- TITANS EDGES CREATION FROM CSV FILES ---------------------------------------*/

println 'linking nodes to nodes...'

// Create edges between each node vertex given links.csv extracted from db_dblinks
// Edge labels represent the relationship between the two linked nodes
new File(csvDir + "links.csv").each({ line ->
    (id, input_id, output_id, label) = line.split(";")

    source = bg.getVertex("node::" + input_id)
    target = bg.getVertex("node::" + output_id)

    bg.addEdge(null, source, target, label)
})

println 'linking users and computers to nodes...'
// Create edges between computer vertices and node vertices given in ComputerToNode.csv (in this file there is no edges
// pointing to null). Edge labels represent the fact that a computer compute a code/data/calculation (a.k.a node)
new File(csvDir + "nodes.csv").each({ line ->
    (node_id, uuid, type, label, description, ctime, mtime, user_id, computer_id, node_version, is_public) = line.split(";")

    node = bg.getVertex("node::" + node_id)

    // Link computers to nodes
    if (computer_id && computer_id != "null")
        computer = bg.getVertex("computer::" + computer_id)
        bg.addEdge(null, computer, node, 'computes')

    // Link users to nodes
    user = bg.getVertex("user::" + user_id)
    bg.addEdge(null, user, node, 'creates')


})

println 'linking nodes to attributes...'

// Create edges between node vertices and attribute vertices extracted from attributes.csv
// Edge labels represent the fact that a node has a given attribute
new File(csvDir + "attributes.csv").each({ line ->
    println line
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(";")

    node = bg.getVertex("node::" + node_id)
    attribute = bg.getVertex("attribute::" + id)

    bg.addEdge(null, node, attribute, 'withAttr')
})

println 'linking nodes to extras...'

// Create edges between node vertices and extra vertices extracted from extras.csv
// Edge labels represent the fact that a node has a given extra attribute
new File(csvDir + "extras.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(";")

    node = bg.getVertex("node::" + node_id)
    extra = bg.getVertex("extra::" + id)

    bg.addEdge(null, node, extra, 'withExtraAttr')
})

println 'linking nodes to calcstates...'

// Create edges between node vertices and calcState vertices extracted from calcstates.csv
// Edge labels represent the fact that a node has a given calculation state
new File(csvDir + "calcstates.csv").each({ line ->
    (id, node_id, state, time) = line.split(";")

    node = bg.getVertex("node::" + node_id)
    calcState = bg.getVertex("calcstate::" + id)

    bg.addEdge(null, node, calcState, 'withCalcState')
})

println 'linking nodes and users to comments...'

// Create edges between node vertices and comment vertices extracted from comments.csv
// Edge labels represent the fact that a node has a given comment
new File(csvDir + "comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(";")

    // Link nodes to comments
    node = bg.getVertex("node::" + node_id)
    comment = bg.getVertex("comment::" + id)
    bg.addEdge(null, node, comment, 'withComment')

    // Link users to comments
    user = bg.getVertex("user::" + user_id)
    comment = bg.getVertex("comment::" + id)
    bg.addEdge(null, user, comment, 'madeComment')
})

println 'linking users to groups...'

// Create edges between user vertices and group vertices extracted from groups.csv
// Edge labels represent the fact that a user is in a given group
new File(csvDir + "groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(";")

    user = bg.getVertex("user::" + user_id)
    group = bg.getVertex("group::" + id)

    bg.addEdge(null, user, group, 'inUserGroup')
})

println 'linking nodes to groups...'

// Create edges between node vertices and group vertices extracted from nodegroups.csv
// Edge labels represent the fact that a node is in a given group
new File(csvDir + "nodegroups.csv").each({ line ->
    (id, group_id, node_id) = line.split(";")

    node = bg.getVertex("node::" + node_id)
    group = bg.getVertex("group::" + id)

    bg.addEdge(null, node, group, 'inNodeGroup')
})

bg.commit()

println 'GRAPH IMPORTED SUCCESSFULLY'