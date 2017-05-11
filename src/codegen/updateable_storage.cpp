//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updateable_storage.cpp
//
// Identification: src/codegen/updateable_storage.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/updateable_storage.h"

#include "codegen/codegen.h"
#include "codegen/if.h"
#include "codegen/type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Add the given type to the storage format. We return the index that this value
// can be found it (i.e., which index to pass into Get() to get the value)
//===----------------------------------------------------------------------===//
uint32_t UpdateableStorage::AddType(type::Type::TypeId type) {
  PL_ASSERT(storage_type_ == nullptr);
  types_.push_back(type);
  return static_cast<uint32_t>(types_.size() - 1);
}

llvm::Type *UpdateableStorage::Finalize(CodeGen &codegen) {

  // Return the constructed type if it has already been finalized
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  std::vector<llvm::Type *> llvm_types;
  const uint32_t num_items = static_cast<uint32_t>(types_.size());

  // Construct the storage for null at the front of the compact storage
  // : we keep no EntryInfo for each bit since it is waste of memory
  llvm::Type *null_bit = codegen.BoolType();
  for (uint32_t i = 0; i < num_items; i++) {
    llvm_types.push_back(null_bit);
  }

  // Construct the storage for the values and create the structure type
  for (uint32_t i = 0; i < num_items; i++) {
    llvm::Type *val_type = nullptr, *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, types_[i], val_type, len_type);

    // 1. Add value type
    // Create a slot metadata entry for the value
    uint32_t val_type_size = static_cast<uint32_t>(codegen.SizeOf(val_type));
    storage_format_.push_back(
        CompactStorage::EntryInfo{val_type, i,
                                  len_type != nullptr ? true : false,
                                  val_type_size});

    // Add the LLVM type of the value into the structure type
    llvm_types.push_back(val_type);

    // If there is a length component, add that too
    if (len_type != nullptr) {
      // Create an entry for the length and add to the struct
      uint32_t len_type_size = static_cast<uint32_t>(codegen.SizeOf(len_type));
      storage_format_.push_back(
          CompactStorage::EntryInfo{len_type, i, true, len_type_size});
      llvm_types.push_back(len_type);
    }
  }
  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = codegen.SizeOf(storage_type_);
  return storage_type_;
}

// Get the value at a specific index into the storage area
codegen::Value UpdateableStorage::GetValueAt(CodeGen &codegen, llvm::Value *ptr,
                                             uint64_t index) const {
  PL_ASSERT(storage_type_ != nullptr);
  PL_ASSERT(index < types_.size());

  // Still linear search
  int32_t val_idx = -1, len_idx = -1;
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    if (storage_format_[i].index == index) {
      val_idx = i;
      if (storage_format_[i].is_var)
        len_idx = i+1;
      break;
    }
  }

  // Make sure we found something
  PL_ASSERT(val_idx >= 0);

  const uint32_t num_items = static_cast<uint32_t>(types_.size());

  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());

  // Load the null-indication bit
  llvm::Value *null_addr =
      codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, index);
  llvm::Value *null = codegen->CreateLoad(null_addr);

  llvm::Value *val_store, *val_null, *val_final;
  llvm::Value *len_store = nullptr, *len_null = nullptr, *len_final = nullptr;
  If is_not_null {codegen, codegen->CreateICmpEQ(null,
                                                 codegen.ConstBool(false))};
  {
    // Load the value
    llvm::Value *val_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, num_items + val_idx);
    val_store = codegen->CreateLoad(val_addr);

    // If there is a length-component for this entry, load it too
    if (len_idx > 0) {
      llvm::Value *len_addr = codegen->CreateConstInBoundsGEP2_32(
          storage_type_, typed_ptr, 0, num_items + len_idx);
      len_store = codegen->CreateLoad(len_addr);
    }
  }
  is_not_null.ElseBlock();
  {
    // Create null values from entry_info.type
    val_null = Type::GetNullLLVMValue(codegen, types_[index]);
    if (len_idx > 0)
      len_null = codegen.Const32(0);
  }
  is_not_null.EndIf();

  val_final = is_not_null.BuildPHI(val_store, val_null);
  if (len_idx > 0)
    len_final = is_not_null.BuildPHI(len_store, len_null);

  // Done
  return codegen::Value{types_[index], val_final, len_final, null};
}

// Get the value at a specific index into the storage area
void UpdateableStorage::SetValueAt(CodeGen &codegen, llvm::Value *ptr,
                                   uint64_t index,
                                   const codegen::Value &value) const {
  llvm::Value *val = nullptr, *len = nullptr, *null = nullptr;
  value.GetValue(val, len, null);

  // This is to protect the cases where some data are coming in without its null
  // bit allocated. These should be changed to ASSERT once we become confident 
  // enough that there are no longer such cases
  if (null == nullptr) {
    null = codegen::Value::SetNullValue(codegen, value);
  }

  // Still linear search
  int32_t val_idx = -1, len_idx = -1;
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    if (storage_format_[i].index == index) {
      val_idx = i;
      if (storage_format_[i].is_var)
        len_idx = i+1;
      break;
    }
  }

  PL_ASSERT(value.GetValue()->getType() == storage_format_[val_idx].type);

  const uint32_t num_items = static_cast<uint32_t>(types_.size());

  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());

  // Write the null-bit
  llvm::Value *null_bit_addr =
      codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, index);
  codegen->CreateStore(null, null_bit_addr);

  If is_not_null {codegen, codegen->CreateICmpEQ(null,
                                                 codegen.ConstBool(false))};
  {
    // Store the value and len at the appropriate slot
    llvm::Value *val_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, num_items + val_idx);
    codegen->CreateStore(val, val_addr);

    // If there's a length-component, store it at the appropriate index too
    if (len != nullptr) {
      llvm::Value *len_addr = codegen->CreateConstInBoundsGEP2_32(
          storage_type_, typed_ptr, 0, num_items + len_idx);
      codegen->CreateStore(len, len_addr);
    }
  }
  is_not_null.EndIf();

}

}  // namespace codegen
}  // namespace peloton
