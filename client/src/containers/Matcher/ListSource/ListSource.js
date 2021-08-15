import React, {Component} from "react";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import ChevronRightIcon from "@material-ui/icons/ChevronRight";
import TableChartIcon from "@material-ui/icons/TableChart";
import TreeView from "@material-ui/lab/TreeView";
import TreeItem from "@material-ui/lab/TreeItem";
import Checkbox from "@material-ui/core/Checkbox";
import classes from "./ListSource.module.css";
import Aux from "../../../hoc/Aux";
import axios from "axios";
import Typography from "@material-ui/core/Typography";
import PostgresLogo from "../../../assets/PostgreSQL-Logo.wine.svg"
import MinioLogo from "../../../assets/minio-1.svg"


class Database {
    constructor(id, name, tables, source) {
        this.id = id;
        this.name = name;
        this.tables = [];
        this.selected = false;
        this.source = source
        this.add_tables(tables);
        if (source === 'postgres') {
            this.provider = PostgresLogo;
        } else if (source === 'minio') {
            this.provider = MinioLogo;
        }
    }
    add_tables(tables){
        tables.map((tableName, index) => this.tables.push(new Table(index, tableName)));
    }
}

class Table {
    constructor(id, name) {
        this.id = id;
        this.name = name;
        this.selected = false;
    }
}

class ListSource extends Component {

    state = {
        dbTree: []
    }

    componentDidMount() {
        axios({
            method: "get",
            url: process.env.REACT_APP_SERVER_ADDRESS + "/matches/holistic/ls_tables"
        }).then(res => {
            const dbTree = [];
            Object.keys(res.data).map((source, index) =>
                res.data[source].map((dbInfo, index2) =>
                    dbTree.push(new Database(index + '_' + index2, dbInfo["db_name"], dbInfo["tables"], source)))
                );
            this.setState({dbTree: dbTree});
        }).catch(err => {
            console.log(err);
        });
    }

    sendSelectedToParent = () => {
        const selectedTables = [];
        this.state.dbTree.map(db =>
            db.tables.map(table =>
                (table.selected) ? selectedTables.push({"db_name": db.name,
                    "table_name": table.name, "source": db.source}) : null));
        this.props.sendSelected(selectedTables);
    }

    handleCheckDB = (dbIdx) => {
        const updatedDBTree = [...this.state.dbTree];
        const dbToUpdate = updatedDBTree.slice(dbIdx, dbIdx+1)[0];
        dbToUpdate.selected = !dbToUpdate.selected;
        dbToUpdate.tables.map(table => table.selected = dbToUpdate.selected);
        updatedDBTree[dbIdx] = dbToUpdate;
        this.setState({dbTree: updatedDBTree},() => this.sendSelectedToParent());
    }

    handleCheckTbl = (dbIdx, tblIdx) => {
        const updatedDBTree = [...this.state.dbTree];
        const dbToUpdate = updatedDBTree.slice(dbIdx, dbIdx+1)[0];
        dbToUpdate.tables[tblIdx].selected = !dbToUpdate.tables[tblIdx].selected;
        updatedDBTree[dbIdx] = dbToUpdate;
        this.setState({dbTree: updatedDBTree}, () => this.sendSelectedToParent());
    }

    renderTree = (dbInfo, index) => {
        return(
            <TreeItem
                key={"d"+dbInfo.id}
                nodeId={"d"+dbInfo.id}
                label={
                    <div className={classes.labelRoot}>
                        <Checkbox
                            checked={dbInfo.selected}
                            onChange={() => this.handleCheckDB(index)}
                            onClick={e => e.stopPropagation()}
                            color="primary"
                        />
                        <img src={dbInfo.provider} alt={"Postgres logo"} width="40px" height="40px" className={classes.labelIcon}/>
                        <Typography variant="body2" className={classes.labelText}>
                            {dbInfo.name}
                        </Typography>
                    </div>
                }
            >
                {dbInfo.tables.map((table, tblIdx) =>
                    < TreeItem
                    key = {"t"+table.id}
                    nodeId = {"t"+table.id}
                    label = {
                        <div className={classes.labelRoot}>
                            <Checkbox
                                checked={table.selected}
                                onChange={() => this.handleCheckTbl(index, tblIdx)}
                                onClick={e => e.stopPropagation()}
                                color="primary"
                            />
                            <TableChartIcon style={{ color: "#016b9f" }} className={classes.labelIcon}/>
                            <Typography variant="body2" className={classes.labelText}>
                                {table.name}
                            </Typography>
                        </div>
                    }
                    />
                )}
            </TreeItem>
        );
    }

    render() {
        return(
            <Aux>
                <div className={classes.ListSource}>
                    <h5>{this.props.header}</h5>
                    <TreeView defaultCollapseIcon={<ExpandMoreIcon />} defaultExpandIcon={<ChevronRightIcon />}>
                        {this.state.dbTree.map((dbInfo, index) => this.renderTree(dbInfo, index))}
                    </TreeView>
                </div>
            </Aux>
        );
    }
}

export default ListSource;
