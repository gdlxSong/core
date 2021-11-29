## patch entity

`json patch` 的核心是以 `value` 作为参数， 通过指定的 `operator` 操作，对 `path` 所在 `JSON-Node` 进行操作。

```json
// 一个patch原子
{
    "path": "string",
    "operator": "string, [replace, remove, add, test, move, copy]",
    "value": "JSON"
}
```



---------------

```json
// The original document

{
  "baz": "qux",
  "foo": "bar"
}

// The patch

[
  { "op": "replace", "path": "/baz", "value": "boo" },
  { "op": "add", "path": "/say", "value": ["world"] },
  { "op": "remove", "path": "/foo" }
]

// The result

{
  "baz": "boo",
  "say": ["world"]
}
```

----------

### patch replace

```json
// The original document

{
    "id": "test123",
    "plugin": "dm",
    "configs": {},
    "properties": {
        "identity": "5130301987120209097765",
        "name": "tony",
        "loveColor": ["black", "red"],
        "grades": {
            "mathematics": 99,
            "history": 99
        }
    }
}

// The patch

[
  { "operator": "replace", "path": "name", "value": "tom" },
  { "operator": "replace", "path": "age", "value": 22 },
  { "operator": "replace", "path": "friends", "value": ["tomas", "jony"] }
  { "operator": "replace", "path": "loveColor[0]", "value": "white" }
  { "operator": "replace", "path": "grades.history", "value": 20 }
]

// The result

{
    "id": "test123",
    "configs": {},
    "properties": {
        "age": 22,
        "friends": ["tomas", "jony"],
        "id": "test123",
        "identity": "5130301987120209097765",
        "name": "tom",
        "loveColor": ["white", "red"],
        "grades": {
            "mathematics": 99,
            "history": 20
        }
    }
}
```



### patch remove



```json
// The original document

{
    "id": "test123",
    "configs": {},
    "properties": {
        "age": 22,
        "friends": ["tomas", "jony"],
        "id": "test123",
        "identity": "5130301987120209097765",
        "name": "tom",
        "loveColor": ["white", "red"],
        "grades": {
            "mathematics": 99,
            "history": 20,
            "biology": {
                "teacher": "Mr.WU",
                "score": 88
            }
        }
    }
}

// The patch

[
  { "operator": "remove", "path": "age", "value": null },
  { "operator": "remove", "path": "loveColor[0]", "value": null },
  { "operator": "remove", "path": "grades.history", "value": null }
  { "operator": "remove", "path": "grades.biology.score", "value": null }
]

// The result

{
    "id": "test123",
    "configs": {},
    "properties": {
        "friends": ["tomas", "jony"],
        "id": "test123",
        "identity": "5130301987120209097765",
        "name": "tom",
        "loveColor": ["red"],
        "grades": {
            "mathematics": 99,
            "biology": {
                "teacher": "Mr.WU"
            }
        }
    }
}
```








### patch add 





```json
// The original document

{
    "id": "test123",
    "configs": {},
    "properties": {
        "friends": ["tomas", "jony"],
        "id": "test123",
        "identity": "5130301987120209097765",
        "name": "tom",
        "loveColor": ["red"],
        "grades": {
            "mathematics": 99,
            "biology": {
                "teacher": "Mr.WU"
            }
        }
    }
}

// The patch

[
  { "operator": "add", "path": "teachers", "value": "Mr.WU" },
  { "operator": "add", "path": "loveColor", "value": "yellow" }
]

// The result

{
    "id": "test123",
    "configs": {},
    "properties": {
        "friends": ["tomas", "jony"],
        "id": "test123",
        "identity": "5130301987120209097765",
        "name": "tom",
        "loveColor": ["red", "yellow"],
        "grades": {
            "mathematics": 99,
            "biology": {
                "teacher": "Mr.WU"
            }
        },
        "teachers": ["Mr.WU"]
    }
}
```






### patch copy

> reversed.



### patch test

> reversed.


### move 

> reversed.


### merge

> reversed.

```json
// The original document

{
    "id": "test123",
    "configs": {},
    "properties": {
        "friends": ["tomas", "jony"],
        "id": "test123",
        "identity": "5130301987120209097765",
        "name": "tom",
        "loveColor": ["red"],
        "grades": {
            "mathematics": 99,
            "biology": {
                "teacher": "Mr.WU"
            }
        }
    }
}

// The patch

[
  { "operator": "merge", "path": "grades", "value": {"Physics": 100} },
  { "operator": "merge", "path": "loveColor", "value": ["yellow"] }
]

// The result

{
    "id": "test123",
    "configs": {},
    "properties": {
        "friends": ["tomas", "jony"],
        "id": "test123",
        "identity": "5130301987120209097765",
        "name": "tom",
        "loveColor": ["red", "yellow"],
        "grades": {
            "Physics": 100,
            "mathematics": 99,
            "biology": {
                "teacher": "Mr.WU"
            }
        },
        "teachers": ["Mr.WU"]
    }
}
```


## reference

- [1] https://datatracker.ietf.org/doc/html/rfc6902 
- [2] http://jsonpatch.com/