import org.apache.tools.ant.types.resources.selectors.Date

// Adapt to the directory where your csv files lie

/*-------------------------------------- TITANS VERTICES CREATION FROM CSV FILES -------------------------------------*/

// Parse db_dbnode entries stored in nodes.csv, creates a node vertex identified by id and add it to TitanGraph

csvDir = "/Users/roger/EPFL/BigData/aiida_titan/"

config = new BaseConfiguration()
config.setProperty("storage.backend", "hbase")

bg = TitanFactory.open(config)

csvDir = "/Users/roger/EPFL/BigData/aiida_titan/"

new File(csvDir + "nodes.csv").each({ line ->
    (id, uuid, type, label, description, ctime, mtime, user_id, computer_id, node_version, is_public) = line.split(",")
    map = [:]
    node = bg.addVertex("node::" + id)
    if (uuid) map.put('uuid', uuid)
    if (type) map.put("type", type)
    if (label) map.put('node_label', label)
    if (description) map.put('description', description)
    if (ctime) map.put('ctime', ctime)
    if (mtime) map.put('mtime', mtime)
    if (user_id) map.put('user_id', user_id)
    if (computer_id && computer_id != "null") map.put('computer_id', computer_id)
    if (node_version) map.put('node_version', node_version)
    if (is_public) map.put('is_public', is_public)


    ElementHelper.setProperties(node, map)
})

// Parse db_dbattribute entries stored in attribute.csv, creates an attribute vertex identified by id
// and add it to TitanGraph. Switch case ensure the correct type for the value
// TODO: Take into account datatype of type 'list' or 'dict'
new File(csvDir + "attributes.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(",")
    node = bg.addVertex("attribute::" + id)

    switch (datatype) {
        case 'tval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":tval.toString()
            ])
            break
        case 'fval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":fval.toFloat()
            ])
            break
        case 'ival':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":ival.toInteger()
            ])
            break
        case 'bval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":bval.toBoolean()
            ])
            break
        case 'dval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":Date.parse("yyyy-MM-dd H:m:s", dval)
            ])
            break
        default:
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":null
            ])
            break
    }
})


// Parse db_dbextra entries stored in extras.csv, creates an extra vertex identified by id and add it to TitanGraph.
// Switch case ensure the correct type for the value
// TODO: Take into account datatype of type 'list' or 'dict'
new File(csvDir + "extras.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(",")
    node = bg.addVertex("extra::" + id)

    switch (datatype) {
        case 'tval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":tval.toString()
            ])
            break
        case 'fval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":fval.toFloat()
            ])
            break
        case 'ival':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":ival.toInteger()
            ])
            break
        case 'bval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":bval.toBoolean()
            ])
            break
        case 'dval':
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":Date.parse("yyyy-MM-dd H:m:s", dval)
            ])
            break
        default:
            ElementHelper.setProperties(node, ["key":key.toString(),
                                               "value":null
            ])
            break
    }
})

// Parse db_dbcalcstate entries stored in calcstates.csv, creates a calcState vertex identified by id
// and add it to TitanGraph
new File(csvDir + "calcstates.csv").each({ line ->
    (id, node_id, state, time) = line.split(",")
    node = bg.addVertex("calcState::" + id)

    ElementHelper.setProperties(node, ["state":id.toInteger(),
                                       "time":Date.parse("yyyy-MM-dd H:m:s", time)
                                ])
})

// Parse db_dbcomment entries stored in comments.csv, creates a comment vertex identified by id and add it to TitanGraph
new File(csvDir + "comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(",")
    node = bg.addVertex("comment::" + id)

    ElementHelper.setProperties(node, ["uuid":uuid.toString(),
                                       "ctime":Date.parse("yyyy-MM-dd H:m:s", ctime),
                                       "mtime":Date.parse("yyyy-MM-dd H:m:s", mtime),
                                       "content":content.toString()
                                ])
})

// Parse db_dbcomputer entries stored in computers.csv, creates a computer vertex identified by id
// and add it to TitanGraph
new File(csvDir + "computers.csv").each({ line ->
    (id, uuid, name, hostname, description, enabled, transport_type,
            scheduler_type, transport_params, metadata) = line.split(",")
    node = bg.addVertex("computer::" + id)

    ElementHelper.setProperties(node, ["uuid":uuid.toString(),
                                       "name":name.toString(),
                                       "hostname": hostname.toString(),
                                       "description":description.toString(),
                                       "enabled":enabled.toBoolean(),
                                       "transport_type":transport_type.toString(),
                                       "scheduler_type":scheduler_type.toString(),
                                       "transport_params":transport_params.toString(),
                                       "metadata":metadata.toString()
                                ])
})

// Parses db_dbgroup entries stored in groups.csv, creates a group vertex identified by id
// and add it to TitanGraph
new File(csvDir + "groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(",")
    node = bg.addVertex("group::" + id)

    ElementHelper.setProperties(node, ["uuid":uuid.toString(),
                                       "name":name.toString(),
                                       "type":type.toString(),
                                       "time":Date.parse("yyyy-MM-dd H:m:s", time),
                                       "description":description.toString()
                                ])
})

// Parse db_dbuser entries stored in users.csv, creates a user vertex identified by id and add it to TitanGraph
new File(csvDir + "users.csv").each({ line ->
    (id, password, last_login, is_superuser, email, first_name, last_name,
            institution, is_staff, is_active, date_joined) = line.split(",")
    node = bg.addVertex("user::" + id)

    ElementHelper.setProperties(node, ["password":password.toString(),
                                       "last_login": Date.parse("yyyy-MM-dd H:m:s", last_login),
                                       "is_superuser":is_superuser.toBoolean(),
                                       "email":email.toString(),
                                       "first_name":first_name.toString(),
                                       "last_name":last_name.toString(),
                                       "institution":institution.toString(),
                                       "is_staff":is_staff.toBoolean(),
                                       "is_active":is_active.toBoolean(),
                                       "date_joined":Date.parse("yyyy-MM-dd H:m:s", date_joined)
    ])
})


/*--------------------------------------- TITANS EDGES CREATION FROM CSV FILES ---------------------------------------*/

// Create edges between each node vertex given links.csv extracted from db_dblinks
// Edge labels represent the relationship between the two linked nodes
new File(csvDir + "links.csv").each({ line ->
    (id, input_id, output_id, label) = line.split(",")

    v1 = bg.getVertex("node::" + input_id)
    v2 = bg.getVertex("node::" + output_id)

    bg.addEdge(null, v1, v2, label)
})

// Create edges between computer vertices and node vertices given in ComputerToNode.csv (in this file there is no edges
// pointing to null). Edge labels represent the fact that a computer compute a code/data/calculation (a.k.a node)
new File(csvDir + "ComputerToNode.csv").each({ line ->
    (computer_id, node_id) = line.split(",")

    computer = bg.getVertex("computer::" + computer_id)
    node = bg.getVertex("node::" + node_id)

    bg.addEdge(null, computer, node, 'computes')
})

// Create edges between user vertices and node vertices extracted from nodes.csv
// Edge labels represent the fact that a user creates a code/data/calculation (a.k.a node)
new File(csvDir + "nodes.csv").each({ line ->
    (id, uuid, type, label, description, ctime, mtime, user_id, computer_id, node_version, is_public) = line.split(",")

    user = bg.getVertex("user::" + user_id)
    node = bg.getVertex("node::" + id)

    bg.addEdge(null, user, node, 'creates')
})

// Create edges between node vertices and attribute vertices extracted from attributes.csv
// Edge labels represent the fact that a node has a given attribute
new File(csvDir + "attributes.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(",")

    node = bg.getVertex("node::" + node_id)
    attribute = bg.getVertex("attribute::" + id)

    bg.addEdge(null, node, attribute, 'withAttr')
})

// Create edges between node vertices and extra vertices extracted from extras.csv
// Edge labels represent the fact that a node has a given extra attribute
new File(csvDir + "extras.csv").each({ line ->
    (id, key, datatype, tval, fval, ival, bval, dval, node_id) = line.split(",")

    node = bg.getVertex("node::" + node_id)
    extra = bg.getVertex("extra::" + id)

    bg.addEdge(null, node, extra, 'withExtraAttr')
})

// Create edges between node vertices and calcState vertices extracted from calcstates.csv
// Edge labels represent the fact that a node has a given calculation state
new File(csvDir + "calcstates.csv").each({ line ->
    (id, node_id, state, time) = line.split(",")

    node = bg.getVertex("node::" + node_id)
    calcState = bg.getVertex("calcState::" + id)

    bg.addEdge(null, node, calcState, 'withCalcState')
})

// Create edges between node vertices and comment vertices extracted from comments.csv
// Edge labels represent the fact that a node has a given comment
new File(csvDir + "comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(",")

    node = bg.getVertex("node::" + node_id)
    comment = bg.getVertex("comment::" + id)

    bg.addEdge(null, node, comment, 'withComment')
})

// Create edges between user vertices and comment vertices extracted from comments.csv
// Edge labels represent the fact that a user has made a given comment
new File(csvDir + "comments.csv").each({ line ->
    (id, uuid, node_id, ctime, mtime, user_id, content) = line.split(",")

    user = bg.getVertex("user::" + user_id)
    comment = bg.getVertex("comment::" + id)

    bg.addEdge(null, node, comment, 'madeComment')
})

// Create edges between user vertices and group vertices extracted from groups.csv
// Edge labels represent the fact that a user is in a given group
new File(csvDir + "groups.csv").each({ line ->
    (id, uuid, name, type, time, description, user_id) = line.split(",")

    user = bg.getVertex("user::" + user_id)
    group = bg.getVertex("group::" + id)

    bg.addEdge(null, user, group, 'inUserGroup')
})

// Create edges between node vertices and group vertices extracted from nodegroups.csv
// Edge labels represent the fact that a node is in a given group
new File(csvDir + "nodegroups.csv").each({ line ->
    (id, group_id, node_id) = line.split(",")

    node = bg.getVertex("node::" + node_id)
    group = bg.getVertex("group::" + id)

    bg.addEdge(null, node, group, 'inNodeGroup')
})

bg.commit()