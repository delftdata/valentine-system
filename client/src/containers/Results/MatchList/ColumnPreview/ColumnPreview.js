import React, {Component} from "react";
import TableContainer from "@material-ui/core/TableContainer";
import classes from "./ColumnPreview.module.css";
import Table from "@material-ui/core/Table";
import TableHead from "@material-ui/core/TableHead";
import Paper from "@material-ui/core/Paper";
import TableBody from "@material-ui/core/TableBody";
import TableRow from "@material-ui/core/TableRow";
import {withStyles} from "@material-ui/core/styles";
import TableCell from "@material-ui/core/TableCell";


const StyledTableCell = withStyles((theme) => ({
  head: {
      backgroundColor: "#534bae",
      border: 1,
      borderRadius: 0,
      boxShadow: "1px 1px 1px 1px rgba(0, 0, 0, 1)",
      color: theme.palette.common.white,
      fontWeight: "bold",
  },
  body: {
    fontSize: 15,
  },
}))(TableCell);


const StyledTableRow = withStyles((theme) => ({
  root: {
    "&:nth-of-type(odd)": {
      backgroundColor: theme.palette.action.hover,
    },
  },
}))(TableRow);


class ColumnPreview extends Component {
    render() {
        return (
            <Paper>
                <TableContainer className={classes.Container}>
                    <Table className={classes.List} size="small">
                        <TableHead>
                            <TableRow>
                                <StyledTableCell align="center">
                                    {this.props.sourceName}
                                </StyledTableCell>
                                <StyledTableCell align="center">
                                    {this.props.targetName}
                                </StyledTableCell>
                              </TableRow>
                        </TableHead>
                        <TableBody>
                            {this.props.sourceData.map((item, index) => (
                                <StyledTableRow key={index}>
                                    <StyledTableCell className={classes.Cell} align="center">
                                        {item}
                                    </StyledTableCell>
                                    <StyledTableCell className={classes.Cell} align="center">
                                        {this.props.targetData[index]}
                                    </StyledTableCell>
                                </StyledTableRow>
                                )
                            )}
                        </TableBody>
                    </Table>
                </TableContainer>
            </Paper>
        );
    }
}

export default ColumnPreview;
