import React from 'react'
import styled from 'styled-components'

const Container = styled.div`
  padding-top: 60px;
`

const DmLayout: React.FC = ({ children }) => {
  return <Container>{children}</Container>
}

export default DmLayout
