import React, { useState, useContext } from 'react'
import styled from 'styled-components'
import { Form, Input, Radio, Button } from 'antd'
import { IPageAction, ITaskInfo } from '../types'
import { EditContext } from '../App'

const formItemLayout = {
  labelCol: {
    xs: { span: 24 },
    sm: { span: 8 }
  },
  wrapperCol: {
    xs: { span: 24 },
    sm: { span: 16 }
  }
}

const tailItemLayout = {
  wrapperCol: {
    xs: { span: 24, offset: 0 },
    sm: { span: 16, offset: 8 }
  }
}

const Container = styled.div`
  max-width: 400px;
  margin: 0 auto;

  button {
    margin-top: 10px;
    margin-right: 40px;
  }
`

type Props = IPageAction<ITaskInfo> & {
  taskInfo: ITaskInfo
}

function NamingStep({ onNext, onPrev, onData, taskInfo }: Props) {
  const edit = useContext(EditContext)
  const [taskName, setTaskName] = useState(taskInfo.taskName)
  const [taskMode, setTaskMode] = useState(taskInfo.taskMode)

  return (
    <Container>
      <Form {...formItemLayout}>
        <Form.Item label="任务名称">
          <Input
            placeholder="test-task"
            value={taskName}
            onChange={(e: any) => setTaskName(e.target.value)}
          />
        </Form.Item>
        <Form.Item label="同步模式">
          <Radio.Group
            disabled={edit}
            onChange={(e: any) => setTaskMode(e.target.value)}
            value={taskMode}
          >
            <Radio value="full">全量</Radio>
            <Radio value="incremental">增量</Radio>
            <Radio value="all">All</Radio>
          </Radio.Group>
        </Form.Item>
        <Form.Item {...tailItemLayout}>
          <Button onClick={() => onPrev()}>取消</Button>
          <Button
            type="primary"
            htmlType="submit"
            onClick={() => {
              onNext()
              onData && onData({ taskName, taskMode })
            }}
            disabled={taskName.length === 0}
          >
            下一步
          </Button>
        </Form.Item>
      </Form>
    </Container>
  )
}

export default NamingStep
