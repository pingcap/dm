import React, { useState } from 'react'
import { Modal, Tree } from 'antd'
import { IFullSchema, IFullTable } from '../types'
import styled from 'styled-components'
import { ALL_DML, DDL_FOR_SCHEMA, DDL_FOR_TABLE } from '../utils/config-util'
const { TreeNode } = Tree

const WaringText = styled.p`
  color: red;
`

type Props = {
  modalVisible: boolean
  onCloseModal: () => void

  targetItem: IFullSchema | IFullTable
  onUpdateItem?: (item: IFullSchema | IFullTable) => void
}

function BinlogFilterModal({
  modalVisible,
  onCloseModal,
  targetItem,
  onUpdateItem
}: Props) {
  const forTable = (targetItem as IFullTable).type === 'table'
  const [checkedKeys, setCheckedKeys] = useState<string[]>(targetItem.filters)
  const [filtersChanged, setFiltersChanged] = useState(false)

  function onCheck(checkedKeys: any) {
    console.log(checkedKeys)
    setCheckedKeys((checkedKeys as string[]).sort())
    setFiltersChanged(targetItem.filters.join('') !== checkedKeys.join(''))
  }

  function onCancel() {
    // reset
    setCheckedKeys(targetItem.filters)
    onCloseModal()
  }

  function onOk() {
    onUpdateItem && onUpdateItem({ ...targetItem, filters: checkedKeys })
    onCloseModal()
  }

  return (
    <Modal
      title={`Binlog 过滤 (${targetItem.key})`}
      visible={modalVisible}
      onCancel={onCancel}
      onOk={onOk}
      okButtonProps={{ disabled: !filtersChanged }}
    >
      {!forTable && (
        <WaringText>
          注意：修改库的过滤规则会重置所有此库的上游表的过滤规则
        </WaringText>
      )}
      <Tree
        checkable
        defaultExpandAll
        checkedKeys={checkedKeys}
        onCheck={onCheck}
      >
        <TreeNode title="all" key="all">
          <TreeNode title="all dml" key="all dml">
            {ALL_DML.map(title => (
              <TreeNode title={title} key={title} />
            ))}
          </TreeNode>
          <TreeNode title="all ddl" key="all ddl">
            {DDL_FOR_SCHEMA.map(ddl => (
              <TreeNode
                title={forTable ? `${ddl} (only for db)` : ddl}
                key={ddl}
                disabled={forTable}
              />
            ))}
            {DDL_FOR_TABLE.map(ddl => (
              <TreeNode title={ddl} key={ddl} />
            ))}
          </TreeNode>
        </TreeNode>
      </Tree>
    </Modal>
  )
}

export default BinlogFilterModal
