import React from "react";
import styled from "styled-components";
import Tooltip from "@material-ui/core/Tooltip";
import withStyles from "@material-ui/core/styles/withStyles";

const GradientPB = styled.div`
  height: 12px;
  margin: 5px;
  background-image: linear-gradient(#bbbbbb, #bbbbbb),
    linear-gradient(
      90deg,
      rgba(255, 0, 0, 1) 0%,
      rgba(255, 255, 0, 1) 50%,
      rgba(0, 255, 0, 1) 100%
    );
  background-size: ${(props) => props.percentage} 100%, 100% 100%;
  background-position: right, left;
  background-repeat: no-repeat;
  }
`;


const DarkTooltip = withStyles((theme) => ({
  arrow: {
    color: theme.palette.common.black
  },
  tooltip: {
    backgroundColor: theme.palette.common.black,
    fontSize: 14
  }
}))(Tooltip);


const GradientProgressBar = (props) => (
    <DarkTooltip arrow title= { props.similarity.toFixed(3)}>
      <GradientPB percentage={100 - props.similarity * 100 + "%"} />
    </DarkTooltip>
);

export default GradientProgressBar;