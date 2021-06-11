import React from "react";

import classes from "./NavigationItems.module.css";
import NavigationItem from "./NavgationItem/NavigationItem";
import Logo from "../../../assets/delft_kiss.png";
import {NavLink} from "react-router-dom";

const navigationItems = (props) => (
    <ul className={classes.NavigationItems}>
        <div>
            <li key={"/"} className={classes.Logo}>
                <NavLink
                    to={"/"}
                    exact
                    activeClassName={classes.active}>
                    <img src={Logo} alt={"Logo"} width="95px" height="60px"/>
                </NavLink>
            </li>
        </div>
        {props.toolbar_elements.map((row) => (
            <NavigationItem key={row.link} link={row.link} exact icon={row.icon}>
                {row.text}
            </NavigationItem>
        ))}
    </ul>
);

export default navigationItems;