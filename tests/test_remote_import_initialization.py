import json
import subprocess
from pathlib import Path


def test_remote_import_initializes_when_dom_ready():
    project_root = Path(__file__).resolve().parents[1]
    upload_js = project_root / "static" / "upload.js"

    node_script = f"""
const fs = require('fs');
const vm = require('vm');
const {{ URL }} = require('url');

const code = fs.readFileSync('{upload_js.as_posix()}', 'utf8');

const formListeners = [];
const buttonListeners = [];
let resetCalled = false;

function makeInput(initialValue = '') {{
    return {{
        value: initialValue,
        classList: {{
            add() {{}},
            remove() {{}}
        }},
        parentElement: {{
            querySelector() {{
                return {{ textContent: '' }};
            }}
        }}
    }};
}}

const elements = {{
    remoteImportForm: {{
        reset() {{ resetCalled = true; }},
        addEventListener(type, handler) {{ formListeners.push(type); }}
    }},
    remoteImportSubmit: {{
        disabled: false,
        addEventListener(type, handler) {{ buttonListeners.push(type); }}
    }},
    remoteImportFormErrors: {{
        classList: {{ add() {{}}, remove() {{}} }},
        textContent: ''
    }},
    remoteImportActivity: {{ style: {{ display: 'none' }} }},
    remoteImportLog: {{
        appendChild() {{}},
        setAttribute() {{}},
        children: [],
        removeChild() {{}},
        scrollTop: 0,
        scrollHeight: 0
    }},
    remoteImportModal: {{
        classList: {{ remove() {{}} }},
        setAttribute() {{}},
        style: {{}},
        querySelector() {{ return null; }}
    }},
    messageContainer: {{
        innerHTML: '',
        appendChild() {{}},
        removeChild() {{}}
    }}
}};

const documentStub = {{
    readyState: 'complete',
    addEventListener(event, handler) {{
        // DOMContentLoaded already fired; nothing to do.
    }},
    getElementById(id) {{
        if (id === 'remoteSourceUrl') return makeInput('https://example.com/video');
        if (id === 'remoteDestinationFolder') return makeInput('/');
        if (id === 'remoteAdvancedCommand') return makeInput('');
        if (elements[id]) return elements[id];
        return null;
    }},
    createElement() {{
        return {{
            className: '',
            innerHTML: '',
            style: {{}},
            appendChild() {{}},
            setAttribute() {{}},
            textContent: ''
        }};
    }},
    querySelectorAll() {{ return []; }}
}};

const context = {{
    console: console,
    document: documentStub,
    window: {{
        preservedSelections: null,
        socketIOConfig: {{}},
        currentFolder: '/',
    }},
    fetch: async () => ({{
        ok: true,
        headers: {{ get: () => 'application/json' }},
        json: async () => ({{ job: {{ id: 'job-123' }} }})
    }}),
    setTimeout,
    clearTimeout,
    setInterval() {{ return 1; }},
    clearInterval() {{}}
}};

context.window.window = context.window;
context.window.jQuery = () => ({{
    on() {{}},
    modal() {{}}
}});
context.window.location = {{ origin: 'http://localhost' }};

vm.createContext(context);
vm.runInContext(code, context);

console.log(JSON.stringify({{
    formListeners,
    buttonListeners,
    resetCalled
}}));
"""

    completed = subprocess.run(
        ["node", "-e", node_script],
        check=True,
        capture_output=True,
        text=True,
    )

    output_line = completed.stdout.strip().splitlines()[-1]
    data = json.loads(output_line)

    assert 'submit' in data['formListeners']
    assert 'click' in data['buttonListeners']
    assert data['resetCalled'] is True


def test_remote_import_normalizes_scheme_before_submit():
    project_root = Path(__file__).resolve().parents[1]
    upload_js = project_root / "static" / "upload.js"

    node_script = f"""
const fs = require('fs');
const vm = require('vm');

const code = fs.readFileSync('{upload_js.as_posix()}', 'utf8');

const fetchCalls = [];

function makeInput(initialValue = '') {{
    return {{
        value: initialValue,
        disabled: false,
        classList: {{
            add() {{}},
            remove() {{}}
        }},
        parentElement: {{
            querySelector() {{
                return {{
                    textContent: '',
                    innerHTML: ''
                }};
            }}
        }}
    }};
}}

const elements = {{
    remoteImportForm: {{
        reset() {{}},
        addEventListener() {{}}
    }},
    remoteImportSubmit: {{
        disabled: false,
        addEventListener() {{}}
    }},
    remoteImportFormErrors: {{
        classList: {{
            add() {{}},
            remove() {{}}
        }},
        textContent: ''
    }},
    remoteImportActivity: {{ style: {{ display: 'none' }} }},
    remoteImportLog: {{
        appendChild() {{}},
        setAttribute() {{}},
        children: [],
        removeChild() {{}},
        scrollTop: 0,
        scrollHeight: 0
    }},
    remoteImportModal: {{
        classList: {{
            remove() {{}}
        }},
        setAttribute() {{}},
        style: {{}},
        querySelector() {{ return null; }}
    }},
    messageContainer: {{
        innerHTML: '',
        appendChild() {{}},
        removeChild() {{}}
    }}
}};

const inputs = {{
    remoteSourceUrl: makeInput(''),
    remoteDestinationFolder: makeInput('/'),
    remoteAdvancedCommand: makeInput('')
}};

const documentStub = {{
    readyState: 'complete',
    body: {{
        classList: {{
            remove() {{}}
        }}
    }},
    addEventListener() {{}},
    getElementById(id) {{
        if (id === 'remoteSourceUrl') return inputs.remoteSourceUrl;
        if (id === 'remoteDestinationFolder') return inputs.remoteDestinationFolder;
        if (id === 'remoteAdvancedCommand') return inputs.remoteAdvancedCommand;
        if (elements[id]) return elements[id];
        return null;
    }},
    createElement() {{
        return {{
            className: '',
            innerHTML: '',
            style: {{}},
            appendChild() {{}},
            setAttribute() {{}},
            textContent: '',
            classList: {{ add() {{}}, remove() {{}} }}
        }};
    }},
    querySelector() {{ return null; }},
    querySelectorAll() {{ return []; }}
}};

const context = {{
    console: console,
    document: documentStub,
    window: {{
        preservedSelections: null,
        socketIOConfig: {{}},
        currentFolder: '/',
        URL: URL,
    }},
    fetch: async (url, options = {{}}) => {{
        fetchCalls.push({{ url, body: options.body }});
        return {{
            ok: true,
            headers: {{ get: () => 'application/json' }},
            json: async () => ({{ job: {{ id: 'job-789' }} }})
        }};
    }},
    setTimeout,
    clearTimeout,
    setInterval() {{ return 1; }},
    clearInterval() {{}},
    URL: URL
}};

context.window.window = context.window;
context.window.jQuery = () => ({{
    on() {{}},
    modal() {{}}
}});

vm.createContext(context);
vm.runInContext(code, context);

context.toggleRemoteImportFormDisabled = () => {{}};
context.closeRemoteImportModal = () => {{}};
context.resetRemoteImportForm = () => {{}};
context.startRemoteImportTracking = () => {{}};

if (context.window.__remoteImport) {{
    context.window.__remoteImport.startRemoteImportTracking = () => {{}};
}}

inputs.remoteSourceUrl.value = 'example.com/video';

context.window.handleRemoteImportSubmit({{
    preventDefault() {{}}
}});

const normalizedValue = inputs.remoteSourceUrl.value;
const body = fetchCalls.length ? fetchCalls[0].body : null;

console.log(JSON.stringify({{
    normalizedValue,
    body
}}));
"""

    completed = subprocess.run(
        ["node", "-e", node_script],
        check=True,
        capture_output=True,
        text=True,
    )

    output_line = completed.stdout.strip().splitlines()[-1]
    data = json.loads(output_line)

    assert data['normalizedValue'] == 'https://example.com/video'
    payload = json.loads(data['body'])
    assert payload['url'] == 'https://example.com/video'
