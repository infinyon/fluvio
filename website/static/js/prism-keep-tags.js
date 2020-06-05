(function () {
    if (!self.Prism) {
        return;
    }

    var div = document.createElement('div');

    Prism.hooks.add('before-highlight', function (env) {
        var elt = env.element;
        if (!elt.hasAttribute('data-keep-tags') && elt.parentNode.tagName.toLowerCase() === 'pre') {
            elt = elt.parentNode;
        }
        var tags = elt.getAttribute('data-keep-tags');
        if (!tags) {
            return;
        }
        var placeholder = elt.getAttribute('data-keep-tags-placeholder') || '___KEEPTAGS{n}___';

        env.keepTags = true;
        env.keepTagsPlaceholder = placeholder;

        tags = tags.split(/\s*,\s*/).join('|');
        var tags_regex = RegExp('<(' + tags + ')>([\\s\\S]*?)</\\1>', 'g');

        env.keepTagsRegex = tags_regex;

        env.tokenStack = [];
        env.backupCode = env.code;

        var code = env.element.innerHTML;
        code = code.replace(tags_regex, function (match) {
            env.tokenStack.push(match);
            return placeholder.replace('{n}', env.tokenStack.length);
        });
        env.element.innerHTML = code;
        code = env.element.textContent;
        code = code.replace(/^(?:\r?\n|\r)/, '');

        env.code = code;
    });

    Prism.hooks.add('after-highlight', function (env) {
        if (!env.keepTags) {
            return;
        }
        for (var i = 0, t; t = env.tokenStack[i]; i++) {

            t = t.replace(env.keepTagsRegex, function (match, tag, inside) {
                div.innerHTML = inside;
                inside = div.textContent;
                return '<' + tag + '>' + Prism.highlight(inside, env.grammar, env.language) + '</' + tag + '>';
            });

            env.highlightedCode = env.highlightedCode.replace(env.keepTagsPlaceholder.replace('{n}', i + 1), t);
            env.element.innerHTML = env.highlightedCode;
        }
    });

}());

Array.prototype.slice.call(document.querySelectorAll("pre,code")).forEach(function (v) {
    v.classList.add("language-sql");
});
Prism.highlightAll();