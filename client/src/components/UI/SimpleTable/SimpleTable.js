import React from "react";
import classes from "./SimpleTable.module.css"

const simpleTable = (props) => (
    <div className={classes.Table}>
        <table>
            <thead>
            <tr>
                {props.head.map(element => (<th> {element} </th>))}
            </tr>
            </thead>
            <tbody>
            {props.body.map(row => (
                <tr>
                    {row.map( element => (<td>{element}</td>))}
                </tr>
            ))}
            </tbody>
        </table>
    </div>

);

export default simpleTable;