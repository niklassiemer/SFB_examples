import numpy as np
import plotly.express as px
from plotly import graph_objects as go
import ipywidgets as wid
import qgrid


class DataExplorer:
    def __init__(self, df, initial_keys=None, debug=True):
        self._df = df.copy()
        self._df.columns = ["".join(index) for index in self._df.columns]
        self._header = wid.HBox()
        self._body = wid.VBox()
        self.debug = debug
        self._output = wid.Output()
        self._box = wid.VBox([self._header, self._body])
        self._column_select = wid.SelectMultiple(description='columns', options=self.df_keys)
        if initial_keys is not None:
            self._column_select.value = initial_keys
        else:
            self._column_select.value = self.df_keys
        self._column_select.observe(self._change_columns)
        self._init_dataframe(self._df)
        self._init_buttons()
        self._change_columns()
        self._show_df()

    def _init_dataframe(self, df):
        self._interactive_df = qgrid.show_grid(df)
        self._displayed_df = self._interactive_df.df

    def _refresh_dataframe(self, df):
        self._init_dataframe(df)
        self._update_key_dependent_buttons()
        self._show_df()

    @property
    def df_keys(self):
        return list(self._df.keys())

    @property
    def _displayed_df_keys(self):
        return list(self._displayed_df.keys())

    def _change_columns(self, event=None):
        self._refresh_dataframe(self._df[list(self._column_select.value)])

    def _update_key_dependent_buttons(self, box=None):
        if box is None:
            box = self._key_dependent_box
        self._name_select = wid.Dropdown(description='Name', options=self._displayed_df_keys)
        self._color_select = wid.Dropdown(description='Color', options=self._displayed_df_keys)
        if 'T' in self._displayed_df_keys:
            self._color_select.value = 'T'
        self._select = wid.SelectMultiple(description='plot', tooltip="Choose 3!", options=self._displayed_df_keys)
        wt_pct_keys = [info for info in self._displayed_df_keys if info.startswith('wt.%')]
        if len(wt_pct_keys) >= 3:
            self._select.value = wt_pct_keys[0:3]

        self._info_select = wid.SelectMultiple(description='info', options=self._displayed_df_keys)
        box.children = tuple([self._name_select, self._select, self._color_select])

    def _init_buttons(self):
        df_button = wid.Button(description='Data')
        df_button.on_click(self._show_df)
        plot_button = wid.Button(description='Ternary Plot')
        plot_button.on_click(self._click_plot)
        self._key_dependent_box = wid.HBox()
        self._update_key_dependent_buttons(self._key_dependent_box)

        self._header.children = tuple([wid.HBox([df_button, self._column_select, plot_button]), self._key_dependent_box])

    def _show_df(self, _=None):
        self._body.children = tuple([self._interactive_df])

    @property
    def _current_i_df(self):
        return self._interactive_df.get_changed_df()

    def _click_plot(self, _=None):
        #a = px.scatter_ternary(self._current_i_df, a='wt.%Mg', b='wt.%Ca', c='wt.%Al',
        #                       color=self._current_i_df['T'].to_list(),
        #                       #color='T',
        #                       hover_name=self._current_i_df['ID'],
        #                       hover_data=self._current_i_df[['wt.%Fe', 'wt.%C', 'wt.%Ti']])
        if len(self._select.value) != 3:
            self._body.children = tuple([wid.HTML("Select exactly 3 quantities!")])
            return
        info_keys = [info for info in self._displayed_df_keys if info.startswith('wt.%')]
        a, b, c = self._select.value[0:3]
        try:
            tern = px.scatter_ternary(self._current_i_df, a=a, b=b, c=c,
                                      color=self._current_i_df[self._color_select.value].to_list(),
                                      labels={'color': self._color_select.value},
                                      size=np.ones(len(self._current_i_df)) * 5,
                                      hover_name=self._current_i_df[self._name_select.value],
                                      hover_data=self._current_i_df[info_keys]
                                      )
        except Exception as e:
            error_msg = f"Error occurred: {e.__class__.__name__}({e})"
            if self.debug:
                line_sep = '\n  '
                error_msg += '<pre>'
                error_msg += f"Debug info (suppress with debug=False):" + line_sep
                error_msg += f"Plot columns {a}, {b}, {c}" + line_sep
                error_msg += f"Use column {self._name_select.value} as the name," + line_sep
                error_msg += f"           {self._color_select.value} for the color scheme" + line_sep
                error_msg += f"           {' ,'.join(info_keys)} for additional hover information." + line_sep
                error_msg += f"Pandas frame 'used':" + line_sep
                error_msg += '</pre> '
                error_msg += self._current_i_df[[a, b, c, self._name_select.value, self._color_select.value] + info_keys].to_html()
            self._body.children = tuple([wid.HTML(error_msg)])
        else:
            self._body.children = tuple([go.FigureWidget(tern)])

    def _ipython_display_(self):
        from IPython import display
        display.display(self._box)