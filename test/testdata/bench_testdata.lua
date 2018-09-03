local json = require('json')
local test_utils = require('test.test_utils')

local bench_testdata = {}

function bench_testdata.get_test_metadata()
    local schemas = json.decode([[{
        "user": {
            "type": "record",
            "name": "user",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "first_name", "type": "string"},
                {"name": "middle_name", "type": "string*"},
                {"name": "last_name", "type": "string"}
            ]
        },
        "user_to_passport": {
            "type": "record",
            "name": "user_to_passport",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "passport_id", "type": "string"}
            ]
        },
        "passport": {
            "type": "record",
            "name": "passport",
            "fields": [
                {"name": "passport_id", "type": "string"},
                {"name": "number", "type": "string"}
            ]
        },
        "user_to_equipment": {
            "type": "record",
            "name": "user_to_equipment",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "equipment_id", "type": "string"}
            ]
        },
        "equipment": {
            "type": "record",
            "name": "equipment",
            "fields": [
                {"name": "equipment_id", "type": "string"},
                {"name": "number", "type": "string"}
            ]
        }
    }]])

    local collections = json.decode([[{
        "user": {
            "schema_name": "user",
            "connections": [
                {
                    "type": "1:1",
                    "name": "user_to_passport_c",
                    "destination_collection": "user_to_passport",
                    "parts": [
                        {
                            "source_field": "user_id",
                            "destination_field": "user_id"
                        }
                    ],
                    "index_name": "user_id"
                },
                {
                    "type": "1:1",
                    "name": "user_to_equipment_c",
                    "destination_collection": "user_to_equipment",
                    "parts": [
                        {
                            "source_field": "user_id",
                            "destination_field": "user_id"
                        }
                    ],
                    "index_name": "user_id"
                }
            ]
        },
        "user_to_passport": {
            "schema_name": "user_to_passport",
            "connections": [
                {
                    "type": "1:1",
                    "name": "passport_c",
                    "destination_collection": "passport",
                    "parts": [
                        {
                            "source_field": "passport_id",
                            "destination_field": "passport_id"
                        }
                    ],
                    "index_name": "passport_id"
                }
            ]
        },
        "user_to_equipment": {
            "schema_name": "user_to_equipment",
            "connections": [
                {
                    "type": "1:1",
                    "name": "equipment_c",
                    "destination_collection": "equipment",
                    "parts": [
                        {
                            "source_field": "equipment_id",
                            "destination_field": "equipment_id"
                        }
                    ],
                    "index_name": "equipment_id"
                }
            ]
        },
        "passport": {
            "schema_name": "passport",
            "connections": []
        },
        "equipment": {
            "schema_name": "equipment",
            "connections": []
        }
    }]])

    local service_fields = {
        user = {},
        user_to_passport = {},
        passport = {},
        user_to_equipment = {},
        equipment = {},
    }

    local indexes = {
        user = {
            user_id = {
                service_fields = {},
                fields = {'user_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
        user_to_passport = {
            primary = {
                service_fields = {},
                fields = {'user_id', 'passport_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            user_id = {
                service_fields = {},
                fields = {'user_id'},
                index_type = 'tree',
                unique = true,
                primary = false,
            },
            passport_id = {
                service_fields = {},
                fields = {'passport_id'},
                index_type = 'tree',
                unique = true,
                primary = false,
            },
        },
        passport = {
            passport_id = {
                service_fields = {},
                fields = {'passport_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
        user_to_equipment = {
            primary = {
                service_fields = {},
                fields = {'user_id', 'equipment_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
            user_id = {
                service_fields = {},
                fields = {'user_id'},
                index_type = 'tree',
                unique = true,
                primary = false,
            },
            equipment_id = {
                service_fields = {},
                fields = {'equipment_id'},
                index_type = 'tree',
                unique = true,
                primary = false,
            },
        },
        equipment = {
            equipment_id = {
                service_fields = {},
                fields = {'equipment_id'},
                index_type = 'tree',
                unique = true,
                primary = true,
            },
        },
    }

    return {
        schemas = schemas,
        collections = collections,
        service_fields = service_fields,
        indexes = indexes,
    }
end

function bench_testdata.init_spaces()
    -- user fields
    local U_USER_ID_FN = 1

    -- user_to_passport fields
    local UTP_USER_ID_FN = 1
    local UTP_PASSPORT_ID_FN = 2

    -- passport fields
    local P_PASSPORT_ID_FN = 1

    -- user_to_equipment fields
    local UTE_USER_ID_FN = 1
    local UTE_EQUIPMENT_ID_FN = 2

    -- equipment fields
    local E_EQUIPMENT_ID_FN = 1

    box.once('init_spaces_bench', function()
        -- user space
        box.schema.create_space('user')
        box.space.user:create_index('user_id',
            {type = 'tree', parts = {
                U_USER_ID_FN, 'string',
            }}
        )

        -- user_to_passport space
        box.schema.create_space('user_to_passport')
        box.space.user_to_passport:create_index('primary',
            {type = 'tree', parts = {
                UTP_USER_ID_FN, 'string',
                UTP_PASSPORT_ID_FN, 'string',
            }}
        )
        box.space.user_to_passport:create_index('user_id',
            {type = 'tree', parts = {
                UTP_USER_ID_FN, 'string',
            }}
        )
        box.space.user_to_passport:create_index('passport_id',
            {type = 'tree', parts = {
                UTP_PASSPORT_ID_FN, 'string',
            }}
        )

        -- passport space
        box.schema.create_space('passport')
        box.space.passport:create_index('passport_id',
            {type = 'tree', parts = {
                P_PASSPORT_ID_FN, 'string',
            }}
        )

        -- user_to_equipment space
        box.schema.create_space('user_to_equipment')
        box.space.user_to_equipment:create_index('primary',
            {type = 'tree', parts = {
                UTE_USER_ID_FN, 'string',
                UTE_EQUIPMENT_ID_FN, 'string',
            }}
        )
        box.space.user_to_equipment:create_index('user_id',
            {type = 'tree', parts = {
                UTE_USER_ID_FN, 'string',
            }}
        )
        box.space.user_to_equipment:create_index('equipment_id',
            {type = 'tree', parts = {
                UTE_EQUIPMENT_ID_FN, 'string',
            }}
        )

        -- equipment space
        box.schema.create_space('equipment')
        box.space.equipment:create_index('equipment_id',
            {type = 'tree', parts = {
                E_EQUIPMENT_ID_FN, 'string',
            }}
        )
    end)
end

function bench_testdata.fill_test_data(shard, meta)
    local virtbox = shard or box.space

    for i = 1, 100 do
        local s = tostring(i)
        test_utils.replace_object(virtbox, meta, 'user', {
            user_id = 'user_id_' .. s,
            first_name = 'first name ' .. s,
            middle_name = box.NULL,
            last_name = 'last name ' .. s,
        })
        test_utils.replace_object(virtbox, meta, 'user_to_passport', {
            user_id = 'user_id_' .. s,
            passport_id = 'passport_id_' .. s,
        })
        test_utils.replace_object(virtbox, meta, 'passport', {
            passport_id = 'passport_id_' .. s,
            number = 'number_' .. s,
        })
        test_utils.replace_object(virtbox, meta, 'user_to_equipment', {
            user_id = 'user_id_' .. s,
            equipment_id = 'equipment_id_' .. s,
        })
        test_utils.replace_object(virtbox, meta, 'equipment', {
            equipment_id = 'equipment_id_' .. s,
            number = 'number_' .. s,
        })
    end
end

function bench_testdata.drop_spaces()
    box.space._schema:delete('onceinit_spaces_bench')
    box.space.user:drop()
    box.space.user_to_passport:drop()
    box.space.passport:drop()
    box.space.user_to_equipment:drop()
    box.space.equipment:drop()
end

return bench_testdata
