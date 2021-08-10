import React, {Component} from "react";
import { withStyles } from "@material-ui/core/styles";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableContainer from "@material-ui/core/TableContainer";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import Paper from "@material-ui/core/Paper";

import classes from "./VerifiedMatches.module.css";
import axios from "axios";
import TablePagination from "@material-ui/core/TablePagination";


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
    fontSize: 14,
  },
}))(TableCell);


const StyledTableRow = withStyles((theme) => ({
  root: {
    "&:nth-of-type(odd)": {
      backgroundColor: theme.palette.action.hover,
    },
  },
}))(TableRow);


class VerifiedMatches extends Component {


    state = {
        page: 0,
        rowsPerPage: 10,
        verifiedMatches: []
    }

    componentDidMount() {
        axios({
                 method: "get",
                 url: process.env.REACT_APP_SERVER_ADDRESS + "/results/verified_matches"
            }).then(res => {
                this.setState({verifiedMatches: res.data});
            }).catch(err => {
                console.log(err);
            })
    }


    handleChangePage = (event, newPage) => {
        this.setState({page: newPage});
    };

    handleChangeRowsPerPage = (event) => {
        this.setState({rowsPerPage: +event.target.value});
        this.setState({page: 0});
    };

    render() {
        return (
            <div className={classes.Parent}>
                <Paper className={classes.Root}>
                 <TableContainer className={classes.Container}>
                  <Table className={classes.VerifiedMatches} size="small">
                    <TableHead>
                      <TableRow>
                          <StyledTableCell align="center" colSpan={4}>Source</StyledTableCell>
                          <StyledTableCell align="center" colSpan={4}>Target</StyledTableCell>
                      </TableRow>
                      <TableRow>
                          <StyledTableCell align="center" colSpan={2}>
                              Table
                          </StyledTableCell>
                          <StyledTableCell align="center" colSpan={2}>
                              Column
                          </StyledTableCell>
                          <StyledTableCell align="center" colSpan={2}>
                              Table
                          </StyledTableCell>
                          <StyledTableCell align="center" colSpan={2}>
                              Column
                          </StyledTableCell>
                      </TableRow>
                      <TableRow>
                          <StyledTableCell align="center">Name</StyledTableCell>
                          <StyledTableCell align="center">GUID</StyledTableCell>
                          <StyledTableCell align="center">Name</StyledTableCell>
                          <StyledTableCell align="center">GUID</StyledTableCell>
                          <StyledTableCell align="center">Name</StyledTableCell>
                          <StyledTableCell align="center">GUID</StyledTableCell>
                          <StyledTableCell align="center">Name</StyledTableCell>
                          <StyledTableCell align="center">GUID</StyledTableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {this.state.verifiedMatches
                          .slice(this.state.page * this.state.rowsPerPage,
                              this.state.page * this.state.rowsPerPage + this.state.rowsPerPage)
                          .map((row, index) => (
                                <StyledTableRow key={index}>
                                    <StyledTableCell align="center">{row["target"]["tbl_nm"]}</StyledTableCell>
                                    <StyledTableCell align="center">{row["target"]["tbl_guid"]}</StyledTableCell>
                                    <StyledTableCell align="center">{row["target"]["clm_nm"]}</StyledTableCell>
                                    <StyledTableCell align="center">{row["target"]["clm_guid"]}</StyledTableCell>
                                    <StyledTableCell align="center">{row["source"]["tbl_nm"]}</StyledTableCell>
                                    <StyledTableCell align="center">{row["source"]["tbl_guid"]}</StyledTableCell>
                                    <StyledTableCell align="center">{row["source"]["clm_nm"]}</StyledTableCell>
                                    <StyledTableCell align="center">{row["source"]["clm_guid"]}</StyledTableCell>
                                </StyledTableRow>
                            ))
                      }
                    </TableBody>
                  </Table>
                </TableContainer>
                <TablePagination
                    rowsPerPageOptions={[10, 25, 100]}
                    component="div"
                    count={this.state.verifiedMatches.length}
                    rowsPerPage={this.state.rowsPerPage}
                    page={this.state.page}
                    onChangePage={this.handleChangePage}
                    onChangeRowsPerPage={this.handleChangeRowsPerPage}
                  />
                </Paper>
            </div>
        );
    }
}

export default VerifiedMatches;