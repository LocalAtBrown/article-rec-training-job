// ==UserScript==
// @name         Jury-rigged Recs
// @namespace    http://tampermonkey.net/
// @version      0.1
// @description  Repl
// @author       You
// @match        https://washingtoncitypaper.com/article/*
// @grant        none
// ==/UserScript==

(function() {
    var recentPosts = document.getElementById("recent-posts-2");
    var title = recentPosts.getElementsByTagName("h2")[0];
    var recs = recentPosts.getElementsByTagName("a");
    title.getElementsByTagName("span")[0].innerHTML = "People Also Read";
    fetch('https://jsonplaceholder.typicode.com/todos/1')
        .then(response => response.json())
        .then(json => {
        for (var i = 0; i < recs.length; i++) {
            recs[i].innerHTML = json.title;
        }
    })
})();
