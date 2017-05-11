//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compact_storage.cpp
//
// Identification: src/codegen/compact_storage.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compact_storage.h"

#include "codegen/codegen.h"
#include "codegen/if.h"
#include "codegen/type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Set up the storage
//===----------------------------------------------------------------------===//
llvm::Type *CompactStorage::Setup(CodeGen &codegen,
    const std::vector<type::Type::TypeId> &types) {

  // Return the constructed type if the compact storage has already been set up
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  types_ = types;

  const auto num_items = types_.size();
  std::vector<llvm::Type *> llvm_types;

  // Construct the storage for null at the front of the compact storage
  // : we keep no EntryInfo for each bit since it is waste of memory
  llvm::Type *null_bit = codegen.BoolType();
  for (uint32_t i = 0; i < num_items; i++) {
    llvm_types.push_back(null_bit);
  }

  // Construct the storage for the values and create the structure type
  for (uint32_t i = 0; i < num_items; i++) {
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    bool is_var = false;
    Type::GetTypeForMaterialization(codegen, types_[i], val_type, len_type);
    if (len_type != nullptr)
      is_var = true;

    // 1. Add value type
    // Create a slot metadata entry for the value
    uint32_t val_type_size = codegen.SizeOf(val_type);
    storage_format_.emplace_back(EntryInfo{val_type, i, is_var, val_type_size});

    // Add the LLVM type of the value into the structure type
    llvm_types.push_back(val_type);

    // 2. Add length type, only if there is length
    if (is_var) {
      // Create a slot metatdata entry for the length
      uint32_t len_type_size = codegen.SizeOf(len_type);
      storage_format_.emplace_back(EntryInfo{len_type, i, false,
                                             len_type_size});
      // Add the LLVM type of the length into the structure type
      llvm_types.push_back(len_type);
    }
  }

  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = codegen.SizeOf(storage_type_);
  return storage_type_;
}

//===----------------------------------------------------------------------===//
// Store the given values into the provided storage area
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::StoreValues(CodeGen &codegen, llvm::Value *ptr,
    const std::vector<codegen::Value> &to_store) const {
  PL_ASSERT(storage_type_ != nullptr);
  PL_ASSERT(to_store.size() == types_.size());

  // Collect all the values and put them into values, lengths and nulls
  const auto num_items = types_.size();
  std::vector<llvm::Value *> vals(num_items), lengths(num_items),
                             nulls(num_items);
  for (uint32_t i = 0; i < num_items; i++) {
    to_store[i].GetValue(vals[i], lengths[i], nulls[i]);
    if (nulls[i] == nullptr)
      nulls[i] = Value::SetNullValue(codegen, to_store[i]);
  }

  // Cast the area pointer to our struct type, removing taking care of offset
  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());

  // Store values in the provided storage area
  // i = position to each value item,
  // j = position to each element in the constructed struct type in storage
  for (uint32_t i = 0, j = 0; i < num_items; i++, j++) {

    // Store the null value in the bitmap
    llvm::Value *null_addr =
        codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, i);
    codegen->CreateStore(nulls[i], null_addr);

    const auto &entry_info = storage_format_[j];
    If is_not_null {codegen, codegen->CreateICmpEQ(nulls[i],
                                                   codegen.ConstBool(false))};
    {
      // Store only when the data is no null
      llvm::Value *addr = codegen->CreateConstInBoundsGEP2_32(storage_type_,
          typed_ptr, 0, num_items+j);
      codegen->CreateStore(vals[entry_info.index], addr);

      // Handle variably sized entries
      if (entry_info.is_var) {
        llvm::Value *len_addr = codegen->CreateConstInBoundsGEP2_32(
            storage_type_, typed_ptr, 0, num_items+(++j));
        codegen->CreateStore(lengths[entry_info.index], len_addr);
      }
    }
    is_not_null.EndIf();
    // skip if it's null
  }

  // Return a pointer into the space just after all the entries we just wrote
  return codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(), ptr,
                                             storage_size_);
}

//===----------------------------------------------------------------------===//
// Load the values stored compactly at the provided storage area into the
// provided vector
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::LoadValues(CodeGen &codegen, llvm::Value *ptr,
    std::vector<codegen::Value> &output) const {

  const auto num_items = types_.size();
  std::vector<llvm::Value *> vals(num_items), lengths(num_items),
                             nulls(num_items);

  // Collect all the values in the provided storage space, separating the values
  // into either value components or length components
  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());
  for (uint32_t i = 0, j = 0; i < num_items; i++, j++) {

    const auto &entry_info = storage_format_[j];
    // Read the bit in the null bitmap
    llvm::Value *null = codegen->CreateLoad(
        codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0,
                                            entry_info.index));
    nulls[entry_info.index] = null;

    If is_not_null {codegen, codegen->CreateICmpEQ(nulls[i],
                                                   codegen.ConstBool(false))};
    {
      // Load the data
      llvm::Value *entry = codegen->CreateLoad(
          codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0,
                                              num_items+j));
      vals[entry_info.index] = entry;
      if (entry_info.is_var) {
        llvm::Value *entry = codegen->CreateLoad(
            codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0,
                                                num_items+(++j)));
        lengths[entry_info.index] = entry;
      }
    }
    is_not_null.ElseBlock();
    {
      // Create null values from entry_info.type
      llvm::Value *entry = Type::GetNullLLVMValue(codegen, types_[i]);
      vals[entry_info.index] = entry;
      if (entry_info.is_var)
        lengths[entry_info.index] = codegen.Const32(0);
    }
    is_not_null.EndIf();
  }

  // Create the output values from those retrieved above
  output.resize(num_items);
  for (uint64_t i = 0; i < num_items; i++) {
    output[i] = codegen::Value::BuildValue(types_[i], vals[i], lengths[i],
                                           nulls[i]);
  }

  // Return a pointer into the space just after all the entries we stored
  return codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(),
      codegen->CreateBitCast(ptr, codegen.CharPtrType()), storage_size_);
}

// Return the maximum possible bytes that this compact storage will need
uint64_t CompactStorage::MaxStorageSize() const {
  return storage_size_;
}

}  // namespace codegen
}  // namespace peloton
