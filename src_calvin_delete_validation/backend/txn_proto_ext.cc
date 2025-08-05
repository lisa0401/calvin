// backend/txn_proto_ext.cc
#include "backend/txn_proto_ext.h"

#include <memory>
#include <iostream>

#include "backend/storage_manager.h"
#include "applications/application.h"
#include "common/types.h"

bool TxnProtoExt::Execute(StorageManager *manager, const Application *app)
{
    if (!AddReadVersions(manager))
    {
        return false;
    }
    app->Execute(this, manager);

    // if (!Validate(manager))
    // {
    //     return false;
    // }

    // ApplyWrites(manager);
    return true;
}

bool TxnProtoExt::AddReadVersions(StorageManager *manager)
{
    clear_read_versions();

    for (const auto &key : read_set())
    {
        std::unique_ptr<Value> val_ptr(manager->ReadObject(key));
        if (val_ptr)
        {
            auto *entry = add_read_versions();
            entry->set_key(key);
            entry->set_version(val_ptr->version);
        }
        else
        {
            std::cerr << "Failed to retrieve version for key: " << key << std::endl;
            clear_read_versions();
            return false;
        }
    }

    for (const auto &key : read_write_set())
    {
        std::unique_ptr<Value> val_ptr(manager->ReadObject(key));
        if (val_ptr)
        {
            auto *entry = add_read_versions();
            entry->set_key(key);
            entry->set_version(val_ptr->version);
        }
        else
        {
            std::cerr << "Failed to retrieve version for key: " << key << std::endl;
            clear_read_versions();
            return false;
        }
    }
    return true;
}

bool TxnProtoExt::Validate(StorageManager *manager) const
{
    for (int i = 0; i < read_versions_size(); ++i)
    {
        const TxnProto_ReadVersion &recorded_read = read_versions(i);
        const std::string &key = recorded_read.key();
        int recorded_version = recorded_read.version();

        std::unique_ptr<Value> current_val_ptr(manager->ReadObject(key));

        if (!current_val_ptr || current_val_ptr->version != recorded_version)
        {
            return false;
        }
    }
    return true;
}

void TxnProtoExt::ApplyWrites(StorageManager *manager)
{
    // Call the new CommitWrites function to persist changes.
    manager->CommitWrites();
}