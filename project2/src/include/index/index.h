/**
 * index.h
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "catalog/schema.h"
#include "table/tuple.h"
#include "type/value.h"

namespace scudb {

/**
 * class IndexMetadata - Holds metadata of an index object
 *
 * 元数据对象维护和索引的元组模式和键属性，
 * 由于外部调用者不知道索引键的实际结构，
 * 因此由索引负责维护这种映射关系，
 * 并进行元组键和索引键之间的转换
 */
class Transaction;
class IndexMetadata {
  IndexMetadata() = delete;

public:
  IndexMetadata(std::string index_name, std::string table_name,
                const Schema *tuple_schema, const std::vector<int> &key_attrs)
      : name_(index_name), table_name_(table_name), key_attrs_(key_attrs) {
    key_schema_ = Schema::CopySchema(tuple_schema, key_attrs_);
  }

  ~IndexMetadata() { delete key_schema_; };

  inline const std::string &GetName() const { return name_; }

  inline const std::string &GetTableName() { return table_name_; }

  // 返回表示索引键的架构对象指针
  inline Schema *GetKeySchema() const { return key_schema_; }

  // 返回索引键内的列数(不是元组键内的列数)注意，
  // 这必须在cpp源文件中定义，因为它使用这里不知道的catalog::Schema成员
  int GetIndexColumnCount() const { return (int)key_attrs_.size(); }

  // 返回索引列和基表列之间的映射关系 
  inline const std::vector<int> &GetKeyAttrs() const { return key_attrs_; }

  // 获取用于调试的字符串表示
  const std::string ToString() const {
    std::stringstream os;

    os << "IndexMetadata["
       << "Name = " << name_ << ", "
       << "Type = B+Tree, "
       << "Table name = " << table_name_ << "] :: ";
    os << key_schema_->ToString();

    return os.str();
  }

private:
  std::string name_;
  std::string table_name_;
  // 键模式和元组模式之间的映射关系
  const std::vector<int> key_attrs_;
  // 索引键的模式
  Schema *key_schema_;
};

/////////////////////////////////////////////////////////////////////
// Index class definition
/////////////////////////////////////////////////////////////////////

/**
 * 索引结构主要维护底层表的模式信息以及索引键和元组键之间的映射关系，
 * 并为外部世界提供了一种与底层索引实现交互的抽象方法，而无需暴露实际实现的接口。
 * 除了简单的插入、删除、谓词插入、点查询和全索引扫描之外，Index object还可以处理谓词扫描。
 * 谓词扫描只支持连接，并且可能优化，也可能不优化，这取决于谓词内部表达式的类型。
 */
class Index {
public:
  Index(IndexMetadata *metadata) : metadata_(metadata) {}

  virtual ~Index() { delete metadata_; }

  // Return the metadata object associated with the index
  IndexMetadata *GetMetadata() const { return metadata_; }

  int GetIndexColumnCount() const { return metadata_->GetIndexColumnCount(); }

  const std::string &GetName() const { return metadata_->GetName(); }

  Schema *GetKeySchema() const { return metadata_->GetKeySchema(); }

  const std::vector<int> &GetKeyAttrs() const {
    return metadata_->GetKeyAttrs();
  }

  // Get a string representation for debugging
  const std::string ToString() const {
    std::stringstream os;

    os << "INDEX: (" << GetName() << ")";
    os << metadata_->ToString();
    return os.str();
  }

  ///////////////////////////////////////////////////////////////////
  // Point Modification
  ///////////////////////////////////////////////////////////////////
  // designed for secondary indexes.
  virtual void InsertEntry(const Tuple &key, RID rid,
                           Transaction *transaction = nullptr) = 0;

  // delete the index entry linked to given tuple
  virtual void DeleteEntry(const Tuple &key,
                           Transaction *transaction = nullptr) = 0;

  virtual void ScanKey(const Tuple &key, std::vector<RID> &result,
                       Transaction *transaction = nullptr) = 0;

private:
  //===--------------------------------------------------------------------===//
  //  Data members
  //===--------------------------------------------------------------------===//
  IndexMetadata *metadata_;
};

} // namespace scudb
