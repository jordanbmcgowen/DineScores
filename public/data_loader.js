/* DineScores data loader - combines all city data files */
window.DATA = [];
if (window.DATA_CHICAGO) window.DATA = window.DATA.concat(window.DATA_CHICAGO);
if (window.DATA_NEW_YORK_1) window.DATA = window.DATA.concat(window.DATA_NEW_YORK_1);
if (window.DATA_NEW_YORK_2) window.DATA = window.DATA.concat(window.DATA_NEW_YORK_2);
if (window.DATA_SAN_FRANCISCO) window.DATA = window.DATA.concat(window.DATA_SAN_FRANCISCO);
if (window.DATA_DALLAS) window.DATA = window.DATA.concat(window.DATA_DALLAS);
