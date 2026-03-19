/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <arrow/c/abi.h>

#include <jni/JniCommon.h>
#include "JniHashTable.h"
#include "folly/String.h"
#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

#ifdef GLUTEN_ENABLE_GPU
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#endif

namespace gluten {

namespace {

facebook::velox::RowVectorPtr materializeHashBuildInput(
    const facebook::velox::RowVectorPtr& rowVector,
    facebook::velox::memory::MemoryPool* memoryPool) {
#ifdef GLUTEN_ENABLE_GPU
  auto cudfVector = std::dynamic_pointer_cast<facebook::velox::cudf_velox::CudfVector>(rowVector);
  if (cudfVector != nullptr) {
    // Broadcast hash build accesses child vectors directly. Materialize a plain Velox RowVector
    // for CudfVector-backed batches before handing them to the hash table builder.
    return facebook::velox::cudf_velox::with_arrow::toVeloxColumn(
        cudfVector->getTableView(),
        memoryPool,
        "",
        cudfVector->stream(),
        cudf::get_current_device_resource_ref());
  }
#endif
  return rowVector;
}

} // namespace

void JniHashTableContext::initialize(JNIEnv* env, JavaVM* javaVm) {
  vm_ = javaVm;
  const char* classSig = "Lorg/apache/gluten/execution/VeloxBroadcastBuildSideCache;";
  jniVeloxBroadcastBuildSideCache_ = createGlobalClassReferenceOrError(env, classSig);
  jniGet_ = getStaticMethodId(env, jniVeloxBroadcastBuildSideCache_, "get", "(Ljava/lang/String;)J");
}

void JniHashTableContext::finalize(JNIEnv* env) {
  if (jniVeloxBroadcastBuildSideCache_ != nullptr) {
    env->DeleteGlobalRef(jniVeloxBroadcastBuildSideCache_);
    jniVeloxBroadcastBuildSideCache_ = nullptr;
  }
}

jlong JniHashTableContext::callJavaGet(const std::string& id) const {
  JNIEnv* env;
  if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    throw gluten::GlutenException("JNIEnv was not attached to current thread");
  }

  const jstring s = env->NewStringUTF(id.c_str());
  auto result = env->CallStaticLongMethod(jniVeloxBroadcastBuildSideCache_, jniGet_, s);
  return result;
}

// Return the velox's hash table.
std::shared_ptr<HashTableBuilder> nativeHashTableBuild(
    const std::vector<std::string>& joinKeys,
    std::vector<std::string> names,
    std::vector<facebook::velox::TypePtr> veloxTypeList,
    int joinType,
    bool hasMixedJoinCondition,
    bool isExistenceJoin,
    bool isNullAwareAntiJoin,
    int64_t bloomFilterPushdownSize,
    std::vector<std::shared_ptr<ColumnarBatch>>& batches,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool) {
  auto rowType = std::make_shared<facebook::velox::RowType>(std::move(names), std::move(veloxTypeList));

  auto sJoin = static_cast<substrait::JoinRel_JoinType>(joinType);
  facebook::velox::core::JoinType vJoin;
  switch (sJoin) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      vJoin = facebook::velox::core::JoinType::kInner;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      vJoin = facebook::velox::core::JoinType::kFull;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      vJoin = facebook::velox::core::JoinType::kLeft;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      vJoin = facebook::velox::core::JoinType::kRight;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
      // Determine the semi join type based on extracted information.
      if (isExistenceJoin) {
        vJoin = facebook::velox::core::JoinType::kLeftSemiProject;
      } else {
        vJoin = facebook::velox::core::JoinType::kLeftSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
      // Determine the semi join type based on extracted information.
      if (isExistenceJoin) {
        vJoin = facebook::velox::core::JoinType::kRightSemiProject;
      } else {
        vJoin = facebook::velox::core::JoinType::kRightSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI: {
      // Determine the anti join type based on extracted information.
      vJoin = facebook::velox::core::JoinType::kAnti;
      break;
    }
    default:
      VELOX_NYI("Unsupported Join type: {}", std::to_string(sJoin));
  }

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>> joinKeyTypes;
  joinKeyTypes.reserve(joinKeys.size());
  for (const auto& name : joinKeys) {
    joinKeyTypes.emplace_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(rowType->findChild(name), name));
  }

  auto hashTableBuilder = std::make_shared<HashTableBuilder>(
      vJoin,
      isNullAwareAntiJoin,
      hasMixedJoinCondition,
      bloomFilterPushdownSize,
      joinKeyTypes,
      rowType,
      memoryPool.get());

  for (auto i = 0; i < batches.size(); i++) {
    auto rowVector = materializeHashBuildInput(
        VeloxColumnarBatch::from(memoryPool.get(), batches[i])->getRowVector(),
        memoryPool.get());
    hashTableBuilder->addInput(rowVector);
  }

  return hashTableBuilder;
}

long getJoin(const std::string& hashTableId) {
  return JniHashTableContext::getInstance().callJavaGet(hashTableId);
}

} // namespace gluten
